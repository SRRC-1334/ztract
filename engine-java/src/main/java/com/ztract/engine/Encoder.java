package com.ztract.engine;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import za.co.absa.cobrix.cobol.parser.Copybook;
import za.co.absa.cobrix.cobol.parser.ast.Primitive;
import za.co.absa.cobrix.cobol.parser.ast.Statement;
import za.co.absa.cobrix.cobol.parser.ast.datatype.CobolType;

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
     */
    public static void encode(String copybookPath, String outputPath,
                              String recfm, Integer lrecl, String codepage)
            throws IOException {

        String copybookContent = new String(Files.readAllBytes(Paths.get(copybookPath)));
        Copybook copybook = CobrixHelper.parseCopybook(copybookContent);

        // Collect primitives
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
                        continue;
                    }

                    int offset = p.binaryProperties().offset();
                    int length = p.binaryProperties().dataSize();

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
        CobolType dataType = primitive.dataType();
        String dataTypeStr = dataType.toString().toUpperCase();

        // ALPHANUMERIC / PIC X
        if (dataTypeStr.contains("ALPHANUMERIC") || dataTypeStr.contains("STRING")) {
            encodeAlphanumeric(element.getAsString(), recordBytes, offset, length, charset);
            return;
        }

        // COMP-3 / PACKED DECIMAL
        if (dataTypeStr.contains("COMP3") || dataTypeStr.contains("COMP-3") || dataTypeStr.contains("PACKED")) {
            BigDecimal value = element.getAsBigDecimal();
            int scale = CobrixHelper.getScale(dataType);
            encodePackedDecimal(value, recordBytes, offset, length, scale);
            return;
        }

        // COMP / COMP-4 / BINARY
        if (dataTypeStr.contains("COMP4") || dataTypeStr.contains("COMP-4")
                || dataTypeStr.contains("BINARY")
                || (dataTypeStr.contains("COMP") && !dataTypeStr.contains("COMP-1") && !dataTypeStr.contains("COMP-2")
                && !dataTypeStr.contains("COMP-3") && !dataTypeStr.contains("COMP3"))) {
            long value = element.getAsLong();
            encodeBinaryInteger(value, recordBytes, offset, length);
            return;
        }

        // COMP-1 -- 4-byte float
        if (dataTypeStr.contains("COMP-1") || dataTypeStr.contains("COMP1") || dataTypeStr.contains("FLOAT")) {
            float value = element.getAsFloat();
            byte[] floatBytes = ByteBuffer.allocate(4).putFloat(value).array();
            System.arraycopy(floatBytes, 0, recordBytes, offset, Math.min(4, length));
            return;
        }

        // COMP-2 -- 8-byte double
        if (dataTypeStr.contains("COMP-2") || dataTypeStr.contains("COMP2") || dataTypeStr.contains("DOUBLE")) {
            double value = element.getAsDouble();
            byte[] doubleBytes = ByteBuffer.allocate(8).putDouble(value).array();
            System.arraycopy(doubleBytes, 0, recordBytes, offset, Math.min(8, length));
            return;
        }

        // NUMERIC DISPLAY -- zoned decimal
        if (dataTypeStr.contains("NUMERIC") || dataTypeStr.contains("DECIMAL")
                || dataTypeStr.contains("INTEGER") || dataTypeStr.contains("INTEGRAL")) {
            BigDecimal value = element.getAsBigDecimal();
            int scale = CobrixHelper.getScale(dataType);
            boolean signed = CobrixHelper.isSigned(dataType);
            encodeZonedDecimal(value, recordBytes, offset, length, scale, signed, charset);
            return;
        }

        // Fallback: alphanumeric
        encodeAlphanumeric(element.getAsString(), recordBytes, offset, length, charset);
    }

    private static void encodeAlphanumeric(String value, byte[] recordBytes,
                                           int offset, int length, Charset charset) {
        byte[] encoded = value.getBytes(charset);
        int copyLen = Math.min(encoded.length, length);
        System.arraycopy(encoded, 0, recordBytes, offset, copyLen);
    }

    private static void encodePackedDecimal(BigDecimal value, byte[] recordBytes,
                                            int offset, int length, int scale) {
        boolean negative = value.signum() < 0;
        BigDecimal absValue = value.abs();

        if (scale > 0) {
            absValue = absValue.movePointRight(scale);
        }

        String digits = absValue.toBigInteger().toString();

        int totalNibbles = length * 2;
        int digitSlots = totalNibbles - 1;

        while (digits.length() < digitSlots) {
            digits = "0" + digits;
        }
        if (digits.length() > digitSlots) {
            digits = digits.substring(digits.length() - digitSlots);
        }

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
                highNibble = digits.charAt(nibbleIndex) - '0';
                lowNibble = negative ? 0x0D : 0x0C;
                nibbleIndex++;
            }

            packed[i] = (byte) ((highNibble << 4) | lowNibble);
        }

        System.arraycopy(packed, 0, recordBytes, offset, length);
    }

    private static void encodeBinaryInteger(long value, byte[] recordBytes,
                                            int offset, int length) {
        for (int i = length - 1; i >= 0; i--) {
            recordBytes[offset + i] = (byte) (value & 0xFF);
            value >>= 8;
        }
    }

    private static void encodeZonedDecimal(BigDecimal value, byte[] recordBytes,
                                           int offset, int length, int scale,
                                           boolean signed, Charset charset) {
        boolean negative = value.signum() < 0;
        BigDecimal absValue = value.abs();

        if (scale > 0) {
            absValue = absValue.movePointRight(scale);
        }

        String digits = absValue.toBigInteger().toString();

        while (digits.length() < length) {
            digits = "0" + digits;
        }
        if (digits.length() > length) {
            digits = digits.substring(digits.length() - length);
        }

        for (int i = 0; i < length; i++) {
            int digit = digits.charAt(i) - '0';
            int zone = 0xF0;

            if (signed && i == length - 1) {
                zone = negative ? 0xD0 : 0xC0;
            }

            recordBytes[offset + i] = (byte) (zone | digit);
        }
    }

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
}
