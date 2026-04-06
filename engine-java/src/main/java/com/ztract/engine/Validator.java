package com.ztract.engine;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import za.co.absa.cobrix.cobol.parser.Copybook;
import za.co.absa.cobrix.cobol.parser.ast.Primitive;
import za.co.absa.cobrix.cobol.parser.ast.Statement;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Validates an EBCDIC file by decoding a sample of records and reporting statistics.
 * Does not write any output files -- reports JSON to stdout.
 */
public class Validator {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Validate an EBCDIC file by decoding up to `sample` records.
     */
    public static void validate(String copybookPath, String inputPath,
                                String recfm, Integer lrecl, String codepage, int sample)
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

        // Per-field statistics
        Map<String, FieldStats> fieldStatsMap = new LinkedHashMap<>();
        for (Primitive p : primitives) {
            fieldStatsMap.put(p.name(), new FieldStats(p.name()));
        }

        int recordsDecoded = 0;
        int recordsWarnings = 0;
        int recordsErrors = 0;

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(inputPath)))) {

            long fileSize = Files.size(Paths.get(inputPath));
            long bytesRead = 0;

            while (bytesRead < fileSize && recordsDecoded < sample) {
                byte[] recordBytes;

                try {
                    if (isVariable) {
                        byte[] rdw = new byte[4];
                        dis.readFully(rdw);
                        bytesRead += 4;

                        int rdwLength = ((rdw[0] & 0xFF) << 8) | (rdw[1] & 0xFF);
                        int dataLength = rdwLength - 4;

                        if (dataLength <= 0) {
                            ZtractEngine.warn("Record " + recordsDecoded + ": invalid RDW length " + rdwLength);
                            recordsErrors++;
                            break;
                        }

                        recordBytes = new byte[dataLength];
                        dis.readFully(recordBytes);
                        bytesRead += dataLength;
                    } else {
                        recordBytes = new byte[recordLength];
                        dis.readFully(recordBytes);
                        bytesRead += recordLength;
                    }
                } catch (java.io.EOFException eof) {
                    ZtractEngine.warn("Unexpected end of file at record " + recordsDecoded);
                    recordsErrors++;
                    break;
                }

                // Decode the record and collect stats
                boolean hadWarning = false;

                for (Primitive p : primitives) {
                    int offset = p.binaryProperties().offset();
                    int length = p.binaryProperties().dataSize();
                    FieldStats stats = fieldStatsMap.get(p.name());

                    if (offset + length > recordBytes.length) {
                        stats.nullCount++;
                        hadWarning = true;
                        continue;
                    }

                    byte[] fieldBytes = new byte[length];
                    System.arraycopy(recordBytes, offset, fieldBytes, 0, length);

                    try {
                        Map<String, Object> tempRecord = Decoder.decodeRecord(recordBytes, List.of(p), charset);
                        Object value = tempRecord.get(p.name());

                        if (value == null) {
                            stats.nullCount++;
                        } else {
                            stats.nonNullCount++;
                            updateStats(stats, value);
                        }
                    } catch (Exception e) {
                        stats.errorCount++;
                        hadWarning = true;
                    }
                }

                if (hadWarning) {
                    recordsWarnings++;
                }

                recordsDecoded++;
            }
        }

        // Build report
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("records_decoded", recordsDecoded);
        report.put("records_warnings", recordsWarnings);
        report.put("records_errors", recordsErrors);

        Map<String, Object> fieldStatsOutput = new LinkedHashMap<>();
        for (Map.Entry<String, FieldStats> entry : fieldStatsMap.entrySet()) {
            fieldStatsOutput.put(entry.getKey(), entry.getValue().toMap());
        }
        report.put("field_stats", fieldStatsOutput);

        System.out.println(GSON.toJson(report));
        System.out.flush();
    }

    private static void updateStats(FieldStats stats, Object value) {
        if (value instanceof Number) {
            double d = ((Number) value).doubleValue();
            if (stats.min == null || d < stats.min) {
                stats.min = d;
            }
            if (stats.max == null || d > stats.max) {
                stats.max = d;
            }
        } else if (value instanceof BigDecimal) {
            double d = ((BigDecimal) value).doubleValue();
            if (stats.min == null || d < stats.min) {
                stats.min = d;
            }
            if (stats.max == null || d > stats.max) {
                stats.max = d;
            }
        } else if (value instanceof String) {
            if (stats.sampleValues.size() < 5) {
                String s = (String) value;
                if (!s.isEmpty() && !stats.sampleValues.contains(s)) {
                    stats.sampleValues.add(s);
                }
            }
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

    private static class FieldStats {
        final String name;
        int nullCount = 0;
        int nonNullCount = 0;
        int errorCount = 0;
        Double min = null;
        Double max = null;
        List<String> sampleValues = new ArrayList<>();

        FieldStats(String name) {
            this.name = name;
        }

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("null_count", nullCount);
            m.put("non_null_count", nonNullCount);
            m.put("error_count", errorCount);
            if (min != null) {
                m.put("min", min);
                m.put("max", max);
            }
            if (!sampleValues.isEmpty()) {
                m.put("sample_values", sampleValues);
            }
            return m;
        }
    }
}
