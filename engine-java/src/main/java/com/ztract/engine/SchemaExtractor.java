package com.ztract.engine;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import za.co.absa.cobrix.cobol.parser.Copybook;
import za.co.absa.cobrix.cobol.parser.ast.Group;
import za.co.absa.cobrix.cobol.parser.ast.Primitive;
import za.co.absa.cobrix.cobol.parser.ast.Statement;
import za.co.absa.cobrix.cobol.parser.ast.datatype.CobolType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import scala.Option;

/**
 * Extracts schema metadata from a COBOL copybook using Cobrix's CopybookParser.
 * Outputs a JSON document describing the record layout.
 */
public class SchemaExtractor {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Parse the copybook and print schema JSON to stdout.
     *
     * @param copybookPath path to the COBOL copybook file
     * @param recfm        record format (F, FB, V, VB, FBA, VBA) or null
     * @param lrecl        logical record length or null
     */
    public static void extract(String copybookPath, String recfm, Integer lrecl) throws IOException {
        String copybookContent = new String(Files.readAllBytes(Paths.get(copybookPath)));

        // Parse the copybook using Cobrix
        Copybook copybook = CobrixHelper.parseCopybook(copybookContent);

        // Build schema output
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("copybook", copybookPath);

        // Compute record length from the AST
        int computedLength = copybook.getRecordSize();

        schema.put("record_length", computedLength);
        schema.put("record_format", recfm != null ? recfm : "FB");

        // Extract fields
        List<Map<String, Object>> fields = new ArrayList<>();
        List<Map<String, Object>> redefinesGroups = new ArrayList<>();

        for (Statement stmt : CobrixHelper.getRootChildren(copybook)) {
            if (stmt instanceof Group) {
                walkGroup((Group) stmt, fields, redefinesGroups);
            } else if (stmt instanceof Primitive) {
                fields.add(extractPrimitiveInfo((Primitive) stmt));
            }
        }

        schema.put("fields", fields);
        schema.put("redefines_groups", redefinesGroups);

        // Warn if lrecl doesn't match computed length
        if (lrecl != null && lrecl != computedLength) {
            ZtractEngine.warn("Specified LRECL (" + lrecl + ") differs from computed record length (" + computedLength + ")");
        }

        System.out.println(GSON.toJson(schema));
        System.out.flush();
    }

    /**
     * Recursively walk a Group node, extracting field info.
     */
    private static void walkGroup(Group group, List<Map<String, Object>> fields,
                                  List<Map<String, Object>> redefinesGroups) {
        // Add the group itself as a field entry
        Map<String, Object> groupInfo = new LinkedHashMap<>();
        groupInfo.put("name", group.name());
        groupInfo.put("level", group.level());
        groupInfo.put("type", "GROUP");
        groupInfo.put("offset", group.binaryProperties().offset());
        groupInfo.put("size", group.binaryProperties().dataSize());

        Option<String> redefines = group.redefines();
        if (redefines.isDefined()) {
            groupInfo.put("redefines", redefines.get());
            // Track redefines group
            Map<String, Object> rg = new LinkedHashMap<>();
            rg.put("name", group.name());
            rg.put("redefines", redefines.get());
            redefinesGroups.add(rg);
        }

        // Check occurs
        Option<Object> occurs = group.occurs();
        if (occurs.isDefined()) {
            groupInfo.put("occurs", occurs.get().toString());
        }

        fields.add(groupInfo);

        // Recurse into children
        for (Statement child : CobrixHelper.getChildren(group)) {
            if (child instanceof Group) {
                walkGroup((Group) child, fields, redefinesGroups);
            } else if (child instanceof Primitive) {
                fields.add(extractPrimitiveInfo((Primitive) child));
            }
        }
    }

    /**
     * Extract metadata from a Primitive (leaf) field.
     */
    private static Map<String, Object> extractPrimitiveInfo(Primitive primitive) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("name", primitive.name());
        info.put("level", primitive.level());

        // Data type classification
        CobolType dataType = primitive.dataType();
        String dataTypeStr = dataType.toString();
        info.put("type", classifyDataType(dataTypeStr));

        // PIC clause
        info.put("pic", CobrixHelper.getPic(dataType));
        info.put("usage", dataTypeStr);

        // Binary properties
        info.put("offset", primitive.binaryProperties().offset());
        info.put("size", primitive.binaryProperties().dataSize());

        // Scale (decimal places)
        info.put("scale", CobrixHelper.getScale(dataType));
        info.put("signed", CobrixHelper.isSigned(dataType));

        // Occurs
        Option<Object> occurs = primitive.occurs();
        if (occurs.isDefined()) {
            info.put("occurs", occurs.get().toString());
        }

        // Redefines
        Option<String> redefines = primitive.redefines();
        if (redefines.isDefined()) {
            info.put("redefines", redefines.get());
        }

        return info;
    }

    /**
     * Classify a Cobrix data type string into a simplified category.
     */
    private static String classifyDataType(String dataType) {
        String upper = dataType.toUpperCase();
        if (upper.contains("ALPHANUMERIC") || upper.contains("STRING")) {
            return "ALPHANUMERIC";
        } else if (upper.contains("PACKED") || upper.contains("COMP3") || upper.contains("COMP-3")) {
            return "PACKED_DECIMAL";
        } else if (upper.contains("BINARY") || upper.contains("COMP4") || upper.contains("COMP-4") || upper.contains("COMP ")) {
            return "BINARY";
        } else if (upper.contains("FLOAT")) {
            return "FLOAT";
        } else if (upper.contains("NUMERIC") || upper.contains("DECIMAL") || upper.contains("INTEGER") || upper.contains("INTEGRAL")) {
            return "NUMERIC";
        }
        return dataType;
    }
}
