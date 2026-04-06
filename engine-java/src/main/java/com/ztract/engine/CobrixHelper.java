package com.ztract.engine;

import za.co.absa.cobrix.cobol.parser.CopybookParser;
import za.co.absa.cobrix.cobol.parser.Copybook;
import za.co.absa.cobrix.cobol.parser.ast.Group;
import za.co.absa.cobrix.cobol.parser.ast.Primitive;
import za.co.absa.cobrix.cobol.parser.ast.Statement;
import za.co.absa.cobrix.cobol.parser.ast.datatype.CobolType;
import za.co.absa.cobrix.cobol.parser.ast.datatype.Decimal;
import za.co.absa.cobrix.cobol.parser.ast.datatype.Integral;

import java.util.ArrayList;
import java.util.List;

import scala.Option;
import scala.collection.JavaConverters;

/**
 * Helper class for calling Cobrix's Scala API from Java.
 * Handles the complex default parameters of CopybookParser.parseTree().
 */
public class CobrixHelper {

    /**
     * Parse a COBOL copybook string using Cobrix's CopybookParser with all default parameters.
     */
    public static Copybook parseCopybook(String copybookContent) {
        return CopybookParser.parseTree(
                copybookContent,
                CopybookParser.parseTree$default$2(),   // dropGroupFillers
                CopybookParser.parseTree$default$3(),   // dropValueFillers
                CopybookParser.parseTree$default$4(),   // fillerNamingPolicy
                CopybookParser.parseTree$default$5(),   // segmentRedefines
                CopybookParser.parseTree$default$6(),   // fieldParentMap
                CopybookParser.parseTree$default$7(),   // stringTrimmingPolicy
                CopybookParser.parseTree$default$8(),   // commentPolicy (boolean)
                CopybookParser.parseTree$default$9(),   // commentPolicy (object)
                CopybookParser.parseTree$default$10(),  // strictSignOverpunch
                CopybookParser.parseTree$default$11(),  // improvedNullDetection
                CopybookParser.parseTree$default$12(),  // ebcdicCodePage
                CopybookParser.parseTree$default$13(),  // asciiCharset
                CopybookParser.parseTree$default$14(),  // codePage
                CopybookParser.parseTree$default$15(),  // asciiCharset
                CopybookParser.parseTree$default$16(),  // isUtf16BigEndian
                CopybookParser.parseTree$default$17(),  // floatingPointFormat
                CopybookParser.parseTree$default$18(),  // nonTerminals
                CopybookParser.parseTree$default$19(),  // occursMappings
                CopybookParser.parseTree$default$20(),  // debugFieldsPolicy
                CopybookParser.parseTree$default$21()   // fieldCodePageMap
        );
    }

    /**
     * Get the children of the root AST group as a Java List.
     */
    public static List<Statement> getRootChildren(Copybook copybook) {
        Group root = copybook.ast();
        return new ArrayList<>(JavaConverters.bufferAsJavaListConverter(root.children()).asJava());
    }

    /**
     * Get the children of a Group as a Java List.
     */
    public static List<Statement> getChildren(Group group) {
        return new ArrayList<>(JavaConverters.bufferAsJavaListConverter(group.children()).asJava());
    }

    /**
     * Get precision (scale) from a CobolType, if applicable.
     * Returns 0 for types without precision.
     */
    public static int getPrecision(CobolType dataType) {
        if (dataType instanceof Decimal) {
            return ((Decimal) dataType).precision();
        } else if (dataType instanceof Integral) {
            return ((Integral) dataType).precision();
        }
        return 0;
    }

    /**
     * Get scale from a CobolType, if applicable.
     * Returns 0 for types without scale.
     */
    public static int getScale(CobolType dataType) {
        if (dataType instanceof Decimal) {
            return ((Decimal) dataType).scale();
        }
        return 0;
    }

    /**
     * Check if the CobolType has a sign position defined.
     */
    public static boolean isSigned(CobolType dataType) {
        if (dataType instanceof Decimal) {
            return ((Decimal) dataType).signPosition().isDefined();
        } else if (dataType instanceof Integral) {
            return ((Integral) dataType).signPosition().isDefined();
        }
        return false;
    }

    /**
     * Get the PIC clause string from the CobolType.
     */
    public static String getPic(CobolType dataType) {
        return dataType.pic();
    }

    /**
     * Check if the CobolType is a COMP-3 (packed decimal) type.
     * Checks if the type is Decimal with compact=Some(Left) which indicates COMP-3.
     */
    public static boolean isComp3(CobolType dataType) {
        if (dataType instanceof Decimal) {
            Decimal dec = (Decimal) dataType;
            // compact() returns Option<Position>; isDefined means it's COMP-3
            return dec.compact().isDefined();
        }
        return false;
    }

    /**
     * Check if the CobolType is a binary (COMP/COMP-4) type.
     * Integral types with compact defined are COMP/COMP-4.
     */
    public static boolean isCompBinary(CobolType dataType) {
        if (dataType instanceof Integral) {
            Integral intg = (Integral) dataType;
            return intg.compact().isDefined();
        }
        return false;
    }

    /**
     * Recursively collect all Primitive fields from the AST.
     */
    public static void collectPrimitives(Statement stmt, List<Primitive> primitives) {
        if (stmt instanceof Primitive) {
            primitives.add((Primitive) stmt);
        } else if (stmt instanceof Group) {
            Group group = (Group) stmt;
            for (Statement child : getChildren(group)) {
                collectPrimitives(child, primitives);
            }
        }
    }
}
