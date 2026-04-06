package com.ztract.engine;

import com.google.gson.Gson;
import za.co.absa.cobrix.cobol.parser.Copybook;
import za.co.absa.cobrix.cobol.parser.ast.Primitive;
import za.co.absa.cobrix.cobol.parser.ast.Statement;
import za.co.absa.cobrix.cobol.parser.ast.datatype.CobolType;

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

/**
 * Decodes EBCDIC binary files into JSON Lines written to stdout.
 * Supports fixed-length (F/FB) and variable-length (V/VB) record formats.
 */
public class Decoder {

    private static final Gson GSON = new Gson();

    /**
     * Decode an EBCDIC file to JSON Lines on stdout.
     */
    public static void decode(String copybookPath, String inputPath,
                              String recfm, Integer lrecl, String codepage, String encoding)
            throws IOException {

        String copybookContent = new String(Files.readAllBytes(Paths.get(copybookPath)));
        Copybook copybook = CobrixHelper.parseCopybook(copybookContent);

        // Collect all primitive fields from the AST
        List<Primitive> primitives = new ArrayList<>();
        for (Statement stmt : CobrixHelper.getRootChildren(copybook)) {
            CobrixHelper.collectPrimitives(stmt, primitives);
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
            int length = p.binaryProperties().dataSize();

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
        CobolType dataType = primitive.dataType();
        String dataTypeStr = dataType.toString().toUpperCase();

        // ALPHANUMERIC / PIC X -- character data
        if (dataTypeStr.contains("ALPHANUMERIC") || dataTypeStr.contains("STRING")) {
            return new String(bytes, charset).stripTrailing();
        }

        // COMP-3 / PACKED DECIMAL
        if (dataTypeStr.contains("COMP3") || dataTypeStr.contains("COMP-3") || dataTypeStr.contains("PACKED")) {
            return decodePackedDecimal(bytes, CobrixHelper.getScale(dataType));
        }

        // COMP / COMP-4 / BINARY -- big-endian integer
        if (dataTypeStr.contains("COMP4") || dataTypeStr.contains("COMP-4")
                || dataTypeStr.contains("BINARY")
                || (dataTypeStr.contains("COMP") && !dataTypeStr.contains("COMP-1") && !dataTypeStr.contains("COMP-2")
                && !dataTypeStr.contains("COMP-3") && !dataTypeStr.contains("COMP3"))) {
            return decodeBinaryInteger(bytes, CobrixHelper.isSigned(dataType));
        }

        // COMP-1 -- 4-byte IEEE float
        if (dataTypeStr.contains("COMP-1") || dataTypeStr.contains("COMP1") || dataTypeStr.contains("FLOAT")) {
            if (bytes.length == 4) {
                return ByteBuffer.wrap(bytes).getFloat();
            }
            return ByteBuffer.wrap(bytes).getDouble();
        }

        // COMP-2 -- 8-byte IEEE double
        if (dataTypeStr.contains("COMP-2") || dataTypeStr.contains("COMP2") || dataTypeStr.contains("DOUBLE")) {
            return ByteBuffer.wrap(bytes).getDouble();
        }

        // NUMERIC DISPLAY -- zoned decimal
        if (dataTypeStr.contains("NUMERIC") || dataTypeStr.contains("DECIMAL")
                || dataTypeStr.contains("INTEGER") || dataTypeStr.contains("INTEGRAL")) {
            return decodeZonedDecimal(bytes, charset, CobrixHelper.getScale(dataType),
                    CobrixHelper.isSigned(dataType));
        }

        // Fallback: treat as alphanumeric
        return new String(bytes, charset).stripTrailing();
    }

    /**
     * Decode COMP-3 packed decimal.
     */
    static BigDecimal decodePackedDecimal(byte[] bytes, int scale) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xFF;
            int highNibble = (b >> 4) & 0x0F;
            int lowNibble = b & 0x0F;

            if (i < bytes.length - 1) {
                sb.append(highNibble);
                sb.append(lowNibble);
            } else {
                sb.append(highNibble);
            }
        }

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

        if (signed && bytes.length > 0 && (bytes[0] & 0x80) != 0) {
            for (int i = bytes.length; i < 8; i++) {
                value |= (0xFFL << (i * 8));
            }
        }

        return value;
    }

    /**
     * Decode zoned decimal (NUMERIC DISPLAY).
     */
    static BigDecimal decodeZonedDecimal(byte[] bytes, Charset charset, int scale, boolean signed) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xFF;
            int digit = b & 0x0F;
            sb.append(digit);
        }

        boolean negative = false;
        if (signed && bytes.length > 0) {
            int zone = (bytes[bytes.length - 1] >> 4) & 0x0F;
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
        CobrixHelper.collectPrimitives(stmt, primitives);
    }

    /**
     * Compute record length from the maximum offset+length of all primitives.
     */
    private static int computeRecordLength(List<Primitive> primitives) {
        int max = 0;
        for (Primitive p : primitives) {
            int end = p.binaryProperties().offset() + p.binaryProperties().dataSize();
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
