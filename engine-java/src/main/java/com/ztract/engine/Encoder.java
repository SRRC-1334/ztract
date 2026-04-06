package com.ztract.engine;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import za.co.absa.cobrix.cobol.parser.CopybookParser;
import za.co.absa.cobrix.cobol.parser.ast.Group;
import za.co.absa.cobrix.cobol.parser.ast.Primitive;
import za.co.absa.cobrix.cobol.parser.ast.Statement;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import scala.collection.JavaConverters;

/**
 * Encodes JSON Lines from stdin into an EBCDIC binary file.
 * Reverse operation of the Decoder.
 */
public class Encoder {

    private static final Gson GSON = new Gson();

    // EBCDIC space character
    private static final byte EBCDIC_SPACE = 0x40;

    /**
     * Read JSON Lines from stdin and write EBCDIC binary records to output file.
     *
     * @param copybookPath path to the COBOL copybook
     * @param outputPath   path to the output binary file
     * @param recfm        record format (F, FB, V, VB, etc.)
     * @param lrecl        logical record length
     * @param codepage     EBCDIC code page (e.g., "cp037")
     */
    public static void encode(String copybookPath, String outputPath,
                              String recfm, Integer lrecl, String codepage)
            throws IOException {

        String copybookContent = new String(Files.readAllBytes(Paths.get(copybookPath)));
        var copybook = CopybookParser.parseTree(copybookContent);

        // Collect primitives
        List<Primitive> primitives = new ArrayList<>();
        for (Statement stmt : JavaConverters.seqAsJavaList(copybook.ast())) {
            Decoder.collectPrimitives(stmt, primitives);
        }

        // Determine record length
        int recordLength;
        if (lrecl != null) {
            recordLength = lrecl;
        } else {
            recordLength = computeRecordLength(primitives);
        }

        boolean isVariable = recfm != null && (recfm.startsWith("V") || recfm.startsWith("v"));
        Charset charset = Decoder.resolveCharset(codepage, "ebcdic");

        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(outputPath)));
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(System.in, StandardCharsets.UTF_8))) {

            String line;
            int recordCount = 0;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                JsonObject json = JsonParser.parseString(line).getAsJsonObject();

                // Allocate record buffer filled with EBCDIC spaces
                byte[] recordBytes = new byte[recordLength];
                Arrays.fill(recordBytes, EBCDIC_SPACE);

                // Encode each field
                for (Primitive p : primitives) {
                    String fieldName = p.name();
                    JsonElement element = json.get(fieldName);

                    if (element == null || element.isJsonNull()) {
                        // Leave as spaces (already filled)
                        continue;
                    }

                    int offset = p.binaryProperties().offset();
                    int length = p.binaryProperties().dataLength();

                    if (offset + length > recordLength) {
                        ZtractEngine.warn("Field '" + fieldName + "' extends beyond record length");
                        continue;
                    }

                    try {
                        encodeField(p, element, recordBytes, offset, length, charset);
                    } catch (Exception e) {
                        ZtractEngine.warn("Field '" + fieldName + "': encode error: " + e.getMessage());
                    }
                }

                // Write the record
                if (isVariable) {
                    // Write RDW: 4 bytes, first 2 = total length (record + RDW), last 2 = 0
                    int totalLength = recordLength + 4;
                    dos.writeByte((totalLength >> 8) & 0xFF);
                    dos.writeByte(totalLength & 0xFF);
                    dos.writeByte(0);
                    dos.writeByte(0);
                }

                dos.write(recordBytes);
                recordCount++;
            }

            dos.flush();
        }
    }

    /**
     * Encode a JSON value into the record byte array for a given field.
     */
    private static void encodeField(Primitive primitive, JsonElement element,
                                    byte[] recordBytes, int offset, int length,
                                    Charset charset) {
        String dataType = primitive.dataType().toString().toUpperCase();

        // ALPHANUMERIC / PIC X
        if (dataType.contains("ALPHANUMERIC") || dataType.contains("STRING")) {
            encodeAlphanumeric(element.getAsString(), recordBytes, offset, length, charset);
            return;
        }

        // COMP-3 / PACKED DECIMAL
        if (dataType.contains("PACKED") || dataType.contains("COMP3") || dataType.contains("COMP-3")) {
            BigDecimal value = element.getAsBigDecimal();
            int scale = primitive.dataType().precision();
            encodePackedDecimal(value, recordBytes, offset, length, scale);
            return;
        }

        // COMP / COMP-4 / BINARY
        if (dataType.contains("BINARY") || dataType.contains("COMP4") || dataType.contains("COMP-4")
                || (dataType.contains("COMP") && !dataType.contains("COMP-1") && !dataType.contains("COMP-2")
                && !dataType.contains("COMP-3"))) {
            long value = element.getAsLong();
            encodeBinaryInteger(value, recordBytes, offset, length);
            return;
        }

        // COMP-1 — 4-byte float
        if (dataType.contains("COMP-1") || dataType.contains("FLOAT")) {
            float value = element.getAsFloat();
            byte[] floatBytes = ByteBuffer.allocate(4).putFloat(value).array();
            System.arraycopy(floatBytes, 0, recordBytes, offset, Math.min(4, length));
            return;
        }

        // COMP-2 — 8-byte double
        if (dataType.contains("COMP-2") || dataType.contains("DOUBLE")) {
            double value = element.getAsDouble();
            byte[] doubleBytes = ByteBuffer.allocate(8).putDouble(value).array();
            System.arraycopy(doubleBytes, 0, recordBytes, offset, Math.min(8, length));
            return;
        }

        // NUMERIC DISPLAY — zoned decimal
        if (dataType.contains("NUMERIC") || dataType.contains("DECIMAL") || dataType.contains("INTEGER")) {
            BigDecimal value = element.getAsBigDecimal();
            int scale = primitive.dataType().precision();
            boolean signed = primitive.dataType().signPosition().isDefined();
            encodeZonedDecimal(value, recordBytes, offset, length, scale, signed, charset);
            return;
        }

        // Fallback: alphanumeric
        encodeAlphanumeric(element.getAsString(), recordBytes, offset, length, charset);
    }

    /**
     * Encode a string as EBCDIC alphanumeric, right-padded with EBCDIC spaces.
     */
    private static void encodeAlphanumeric(String value, byte[] recordBytes,
                                           int offset, int length, Charset charset) {
        byte[] encoded = value.getBytes(charset);
        int copyLen = Math.min(encoded.length, length);
        System.arraycopy(encoded, 0, recordBytes, offset, copyLen);
        // Remaining bytes are already EBCDIC spaces from initialization
    }

    /**
     * Encode a value as COMP-3 packed decimal.
     */
    private static void encodePackedDecimal(BigDecimal value, byte[] recordBytes,
                                            int offset, int length, int scale) {
        boolean negative = value.signum() < 0;
        BigDecimal absValue = value.abs();

        // Scale to integer
        if (scale > 0) {
            absValue = absValue.movePointRight(scale);
        }

        String digits = absValue.toBigInteger().toString();

        // Packed decimal: each byte = 2 digits, last nibble = sign
        // Total nibbles = (length * 2), digits = (length * 2 - 1)
        int totalNibbles = length * 2;
        int digitSlots = totalNibbles - 1;

        // Pad digits with leading zeros
        while (digits.length() < digitSlots) {
            digits = "0" + digits;
        }
        // Truncate if too long
        if (digits.length() > digitSlots) {
            digits = digits.substring(digits.length() - digitSlots);
        }

        // Pack the digits
        byte[] packed = new byte[length];
        int nibbleIndex = 0;
        for (int i = 0; i < length; i++) {
            int highNibble;
            int lowNibble;

            if (i < length - 1) {
                highNibble = digits.charAt(nibbleIndex) - '0';
                lowNibble = digits.charAt(nibbleIndex + 1) - '0';
                nibbleIndex += 2;
            } else {
                // Last byte: digit + sign
                highNibble = digits.charAt(nibbleIndex) - '0';
                lowNibble = negative ? 0x0D : 0x0C;
                nibbleIndex++;
            }

            packed[i] = (byte) ((highNibble << 4) | lowNibble);
        }

        System.arraycopy(packed, 0, recordBytes, offset, length);
    }

    /**
     * Encode a long value as big-endian binary (COMP/COMP-4).
     */
    private static void encodeBinaryInteger(long value, byte[] recordBytes,
                                            int offset, int length) {
        for (int i = length - 1; i >= 0; i--) {
            recordBytes[offset + i] = (byte) (value & 0xFF);
            value >>= 8;
        }
    }

    /**
     * Encode a value as zoned decimal (NUMERIC DISPLAY).
     */
    private static void encodeZonedDecimal(BigDecimal value, byte[] recordBytes,
                                           int offset, int length, int scale,
                                           boolean signed, Charset charset) {
        boolean negative = value.signum() < 0;
        BigDecimal absValue = value.abs();

        if (scale > 0) {
            absValue = absValue.movePointRight(scale);
        }

        String digits = absValue.toBigInteger().toString();

        // Pad with leading zeros
        while (digits.length() < length) {
            digits = "0" + digits;
        }
        if (digits.length() > length) {
            digits = digits.substring(digits.length() - length);
        }

        // Encode each digit as EBCDIC zoned decimal: zone nibble (0xF) + digit nibble
        for (int i = 0; i < length; i++) {
            int digit = digits.charAt(i) - '0';
            int zone = 0xF0;

            // For signed fields, the zone of the last byte indicates the sign
            if (signed && i == length - 1) {
                zone = negative ? 0xD0 : 0xC0;
            }

            recordBytes[offset + i] = (byte) (zone | digit);
        }
    }

    /**
     * Compute record length from primitive fields.
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
}
