package com.ztract.engine;

import com.google.gson.Gson;
import za.co.absa.cobrix.cobol.parser.CopybookParser;
import za.co.absa.cobrix.cobol.parser.ast.Group;
import za.co.absa.cobrix.cobol.parser.ast.Primitive;
import za.co.absa.cobrix.cobol.parser.ast.Statement;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import scala.Option;
import scala.collection.JavaConverters;

/**
 * Decodes EBCDIC binary files into JSON Lines written to stdout.
 * Supports fixed-length (F/FB) and variable-length (V/VB) record formats.
 */
public class Decoder {

    private static final Gson GSON = new Gson();

    /**
     * Decode an EBCDIC file to JSON Lines on stdout.
     *
     * @param copybookPath path to the COBOL copybook
     * @param inputPath    path to the binary input file
     * @param recfm        record format (F, FB, V, VB, etc.)
     * @param lrecl        logical record length (required for fixed formats)
     * @param codepage     EBCDIC code page (e.g., "cp037")
     * @param encoding     encoding type ("ebcdic" or "ascii")
     */
    public static void decode(String copybookPath, String inputPath,
                              String recfm, Integer lrecl, String codepage, String encoding)
            throws IOException {

        String copybookContent = new String(Files.readAllBytes(Paths.get(copybookPath)));
        var copybook = CopybookParser.parseTree(copybookContent);

        // Collect all primitive fields from the AST
        List<Primitive> primitives = new ArrayList<>();
        for (Statement stmt : JavaConverters.seqAsJavaList(copybook.ast())) {
            collectPrimitives(stmt, primitives);
        }

        // Determine record length
        int recordLength;
        if (lrecl != null) {
            recordLength = lrecl;
        } else {
            recordLength = computeRecordLength(primitives);
        }

        boolean isVariable = recfm != null && (recfm.startsWith("V") || recfm.startsWith("v"));
        Charset charset = resolveCharset(codepage, encoding);

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(inputPath)))) {

            long fileSize = Files.size(Paths.get(inputPath));
            long bytesRead = 0;
            int recordCount = 0;

            while (bytesRead < fileSize) {
                byte[] recordBytes;

                if (isVariable) {
                    // Variable-length: read 4-byte RDW (Record Descriptor Word)
                    byte[] rdw = new byte[4];
                    dis.readFully(rdw);
                    bytesRead += 4;

                    // RDW: first 2 bytes = record length (including RDW), big-endian
                    int rdwLength = ((rdw[0] & 0xFF) << 8) | (rdw[1] & 0xFF);
                    int dataLength = rdwLength - 4;

                    if (dataLength <= 0) {
                        ZtractEngine.warn("Record " + recordCount + ": invalid RDW length " + rdwLength);
                        break;
                    }

                    recordBytes = new byte[dataLength];
                    dis.readFully(recordBytes);
                    bytesRead += dataLength;
                } else {
                    // Fixed-length: read exactly recordLength bytes
                    recordBytes = new byte[recordLength];
                    dis.readFully(recordBytes);
                    bytesRead += recordLength;
                }

                // Decode the record
                Map<String, Object> record = decodeRecord(recordBytes, primitives, charset);

                // Write JSON line to stdout
                System.out.println(GSON.toJson(record));
                System.out.flush();

                recordCount++;
            }
        }
    }

    /**
     * Decode a single record's bytes into a map of field values.
     */
    static Map<String, Object> decodeRecord(byte[] recordBytes, List<Primitive> primitives,
                                            Charset charset) {
        Map<String, Object> record = new LinkedHashMap<>();

        for (Primitive p : primitives) {
            int offset = p.binaryProperties().offset();
            int length = p.binaryProperties().dataLength();

            // Bounds check
            if (offset + length > recordBytes.length) {
                ZtractEngine.warn("Field '" + p.name() + "' extends beyond record boundary");
                record.put(p.name(), null);
                continue;
            }

            byte[] fieldBytes = new byte[length];
            System.arraycopy(recordBytes, offset, fieldBytes, 0, length);

            try {
                Object value = decodeField(p, fieldBytes, charset);
                record.put(p.name(), value);
            } catch (Exception e) {
                ZtractEngine.warn("Field '" + p.name() + "': decode error: " + e.getMessage());
                record.put(p.name(), null);
            }
        }

        return record;
    }

    /**
     * Decode a single field's bytes based on its COBOL data type.
     */
    private static Object decodeField(Primitive primitive, byte[] bytes, Charset charset) {
        String dataType = primitive.dataType().toString().toUpperCase();

        // ALPHANUMERIC / PIC X — character data
        if (dataType.contains("ALPHANUMERIC") || dataType.contains("STRING")) {
            return new String(bytes, charset).stripTrailing();
        }

        // COMP-3 / PACKED DECIMAL
        if (dataType.contains("PACKED") || dataType.contains("COMP3") || dataType.contains("COMP-3")) {
            return decodePackedDecimal(bytes, primitive.dataType().precision());
        }

        // COMP / COMP-4 / BINARY — big-endian integer
        if (dataType.contains("BINARY") || dataType.contains("COMP4") || dataType.contains("COMP-4")
                || (dataType.contains("COMP") && !dataType.contains("COMP-1") && !dataType.contains("COMP-2")
                && !dataType.contains("COMP-3"))) {
            return decodeBinaryInteger(bytes, primitive.dataType().signPosition().isDefined());
        }

        // COMP-1 — 4-byte IEEE float
        if (dataType.contains("COMP-1") || dataType.contains("FLOAT")) {
            if (bytes.length == 4) {
                return ByteBuffer.wrap(bytes).getFloat();
            }
            return ByteBuffer.wrap(bytes).getDouble();
        }

        // COMP-2 — 8-byte IEEE double
        if (dataType.contains("COMP-2") || dataType.contains("DOUBLE")) {
            return ByteBuffer.wrap(bytes).getDouble();
        }

        // NUMERIC DISPLAY — zoned decimal (EBCDIC digits)
        if (dataType.contains("NUMERIC") || dataType.contains("DECIMAL") || dataType.contains("INTEGER")) {
            return decodeZonedDecimal(bytes, charset, primitive.dataType().precision(),
                    primitive.dataType().signPosition().isDefined());
        }

        // Fallback: treat as alphanumeric
        return new String(bytes, charset).stripTrailing();
    }

    /**
     * Decode COMP-3 packed decimal.
     * Each byte has two BCD digits (nibbles). The last nibble is the sign.
     * 0x0C = positive, 0x0D = negative, 0x0F = unsigned.
     */
    static BigDecimal decodePackedDecimal(byte[] bytes, int scale) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xFF;
            int highNibble = (b >> 4) & 0x0F;
            int lowNibble = b & 0x0F;

            if (i < bytes.length - 1) {
                // Both nibbles are digits
                sb.append(highNibble);
                sb.append(lowNibble);
            } else {
                // Last byte: high nibble is digit, low nibble is sign
                sb.append(highNibble);
            }
        }

        // Determine sign from last nibble of last byte
        int signNibble = bytes[bytes.length - 1] & 0x0F;
        boolean negative = (signNibble == 0x0D);

        String digits = sb.toString();
        if (digits.isEmpty()) {
            return BigDecimal.ZERO;
        }

        BigDecimal value = new BigDecimal(new BigInteger(digits));
        if (scale > 0) {
            value = value.movePointLeft(scale);
        }
        if (negative) {
            value = value.negate();
        }

        return value;
    }

    /**
     * Decode a big-endian binary integer (COMP/COMP-4).
     */
    static long decodeBinaryInteger(byte[] bytes, boolean signed) {
        long value = 0;
        for (byte b : bytes) {
            value = (value << 8) | (b & 0xFF);
        }

        // Handle sign extension for signed values
        if (signed && bytes.length > 0 && (bytes[0] & 0x80) != 0) {
            // Sign extend
            for (int i = bytes.length; i < 8; i++) {
                value |= (0xFFL << (i * 8));
            }
        }

        return value;
    }

    /**
     * Decode zoned decimal (NUMERIC DISPLAY).
     * In EBCDIC, digits 0-9 are 0xF0-0xF9. The sign is in the zone of the last byte.
     */
    static BigDecimal decodeZonedDecimal(byte[] bytes, Charset charset, int scale, boolean signed) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xFF;
            int digit = b & 0x0F;
            sb.append(digit);
        }

        // Check sign from zone nibble of last byte
        boolean negative = false;
        if (signed && bytes.length > 0) {
            int zone = (bytes[bytes.length - 1] >> 4) & 0x0F;
            // 0xD = negative in EBCDIC
            negative = (zone == 0x0D);
        }

        String digits = sb.toString();
        BigDecimal value = new BigDecimal(new BigInteger(digits));
        if (scale > 0) {
            value = value.movePointLeft(scale);
        }
        if (negative) {
            value = value.negate();
        }

        return value;
    }

    /**
     * Recursively collect all Primitive fields from the AST.
     */
    static void collectPrimitives(Statement stmt, List<Primitive> primitives) {
        if (stmt instanceof Primitive) {
            primitives.add((Primitive) stmt);
        } else if (stmt instanceof Group) {
            Group group = (Group) stmt;
            for (Statement child : JavaConverters.seqAsJavaList(group.children())) {
                collectPrimitives(child, primitives);
            }
        }
    }

    /**
     * Compute record length from the maximum offset+length of all primitives.
     */
    private static int computeRecordLength(List<Primitive> primitives) {
        int max = 0;
        for (Primitive p : primitives) {
            int end = p.binaryProperties().offset() + p.binaryProperties().dataLength();
            if (end > max) {
                max = end;
            }
        }
        return max;
    }

    /**
     * Resolve the charset from codepage and encoding parameters.
     */
    static Charset resolveCharset(String codepage, String encoding) {
        if ("ascii".equalsIgnoreCase(encoding)) {
            return Charset.forName("US-ASCII");
        }
        // EBCDIC code pages: cp037, cp500, cp1047, cp277, etc.
        try {
            return Charset.forName(codepage.toUpperCase().replace("CP", "IBM"));
        } catch (Exception e) {
            try {
                return Charset.forName(codepage);
            } catch (Exception e2) {
                ZtractEngine.warn("Unknown codepage '" + codepage + "', falling back to CP037");
                return Charset.forName("IBM037");
            }
        }
    }
}
