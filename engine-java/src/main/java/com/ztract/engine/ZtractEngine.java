package com.ztract.engine;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Main entry point for the Ztract engine.
 * Thin CLI around Cobrix's cobol-parser, invoked by Python's ZtractBridge.
 */
public class ZtractEngine {

    private static final String VERSION = "0.1.0";

    public static void main(String[] args) {
        // Ensure stdout is UTF-8
        PrintStream stdout = new PrintStream(System.out, false, StandardCharsets.UTF_8);
        System.setOut(stdout);

        try {
            Map<String, String> opts = parseArgs(args);

            // Validate required args
            String copybook = opts.get("copybook");
            if (copybook == null) {
                error("--copybook is required");
                System.exit(1);
            }

            boolean schemaOnly = opts.containsKey("schema-only");
            String mode = opts.getOrDefault("mode", "decode");
            String recfm = opts.get("recfm");
            Integer lrecl = opts.containsKey("lrecl") ? Integer.parseInt(opts.get("lrecl")) : null;
            String codepage = opts.getOrDefault("codepage", "cp037");
            String encoding = opts.getOrDefault("encoding", "ebcdic");

            if (schemaOnly) {
                SchemaExtractor.extract(copybook, recfm, lrecl);
                System.exit(0);
            }

            switch (mode) {
                case "decode": {
                    String input = opts.get("input");
                    if (input == null) {
                        error("--input is required for decode mode");
                        System.exit(1);
                    }
                    Decoder.decode(copybook, input, recfm, lrecl, codepage, encoding);
                    break;
                }
                case "encode": {
                    String output = opts.get("output");
                    if (output == null) {
                        error("--output is required for encode mode");
                        System.exit(1);
                    }
                    Encoder.encode(copybook, output, recfm, lrecl, codepage);
                    break;
                }
                case "validate": {
                    String input = opts.get("input");
                    if (input == null) {
                        error("--input is required for validate mode");
                        System.exit(1);
                    }
                    int sample = 1000;
                    if (opts.containsKey("sample")) {
                        sample = Integer.parseInt(opts.get("sample"));
                    }
                    Validator.validate(copybook, input, recfm, lrecl, codepage, sample);
                    break;
                }
                default:
                    error("Unknown mode: " + mode);
                    System.exit(1);
            }

            System.out.flush();
            System.exit(0);

        } catch (NumberFormatException e) {
            error("Invalid numeric argument: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            error(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Parse command-line arguments into a map.
     * Flags (no value) are stored with empty string value.
     */
    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> opts = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                String key = arg.substring(2);
                // Check if this is a flag (no value) or has a value
                if (key.equals("schema-only")) {
                    opts.put(key, "");
                } else if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    opts.put(key, args[i + 1]);
                    i++;
                } else {
                    opts.put(key, "");
                }
            }
        }
        return opts;
    }

    /** Print an error message to stderr. */
    public static void error(String msg) {
        System.err.println("ERROR: " + msg);
    }

    /** Print a warning message to stderr. */
    public static void warn(String msg) {
        System.err.println("WARN: " + msg);
    }
}
