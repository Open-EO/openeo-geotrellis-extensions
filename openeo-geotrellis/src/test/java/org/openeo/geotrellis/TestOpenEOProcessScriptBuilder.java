package org.openeo.geotrellis;

import geotrellis.raster.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Function1;
import scala.Int;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class TestOpenEOProcessScriptBuilder {

    private void testNdvi(OpenEOProcessScriptBuilder builder) {
        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        DoubleArrayTile tile1 = fillDoubleArrayTile(4, 2, 3, 10, 6, 3, 9, 15, 0, Double.NaN);
        DoubleArrayTile tile2 = fillDoubleArrayTile(4, 2, 0, 6, 10, 9, 7, 17, 0, Double.NaN);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1, tile2)));
        assertEquals(1, result.length());
        Tile ndvi = result.apply(0);
        assertDoubleTileEquals(fillDoubleArrayTile(4, 2, 1.0, 0.25, -0.25, -0.5, 0.125, -0.0625, Double.NaN, Double.NaN), ndvi);
    }

    static OpenEOProcessScriptBuilder createNormalizedDifferenceProcessLegacy() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> empty = Collections.emptyMap();
        builder.expressionStart("divide", dummyMap("data"));
        builder.arrayStart("data");

        builder.expressionStart("subtract", dummyMap("data"));
        builder.argumentStart("data");
        builder.argumentEnd();
        builder.expressionEnd("subtract", dummyMap("data"));
        builder.arrayElementDone();

        builder.expressionStart("sum", dummyMap("data"));
        builder.argumentStart("data");
        builder.argumentEnd();
        builder.expressionEnd("sum", dummyMap("data"));
        builder.arrayElementDone();

        builder.arrayEnd();
        builder.expressionEnd("divide", dummyMap("data"));
        return builder;
    }

    @DisplayName("Test NDVI legacy style")
    @Test
    public void testNdviLegacy() {
        testNdvi(createNormalizedDifferenceProcessLegacy());
    }

    /**
     * NDVI implementation with "sum(data)", "subtract(data)" and "divide(data)" (API 0.4 style)
     */
    static OpenEOProcessScriptBuilder createNormalizedDifferenceProcess04DataArrays() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> empty = Collections.emptyMap();
        builder.expressionStart("divide", dummyMap("data"));
        builder.arrayStart("data");

        builder.expressionStart("subtract", dummyMap("data"));
        buildBandArray(builder, "data");
        builder.expressionEnd("subtract", dummyMap("data"));
        builder.arrayElementDone();

        builder.expressionStart("sum", dummyMap("data"));
        buildBandArray(builder, "data");
        builder.expressionEnd("sum", dummyMap("data"));
        builder.arrayElementDone();

        builder.arrayEnd();
        builder.expressionEnd("divide", dummyMap("data"));
        return builder;
    }

    @DisplayName("Test NDVI 0.4-style with 'sum/subtract/divide(data)'")
    @Test
    public void testNDVI04DataArrays() {
        testNdvi(createNormalizedDifferenceProcess04DataArrays());
    }

    /**
     * NDVI implementation with "add(x,y)", "subtract(x,y)" and "divide(x,y)" (API 1.0 style)
     */
    static OpenEOProcessScriptBuilder createNormalizedDifferenceProcess10AddXY() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> empty = Collections.emptyMap();
        builder.expressionStart("divide", dummyMap("x", "y"));

        builder.argumentStart("x");
        builder.expressionStart("subtract", dummyMap("x", "y"));
        buildBandXYArguments(builder, 0, 1);
        builder.expressionEnd("subtract", dummyMap("x", "y"));
        builder.argumentEnd();

        builder.argumentStart("y");
        builder.expressionStart("add", dummyMap("x", "y"));
        buildBandXYArguments(builder, 0, 1);
        builder.expressionEnd("add", dummyMap("x", "y"));
        builder.argumentEnd();

        builder.expressionEnd("divide", dummyMap("x", "y"));
        return builder;
    }

    @DisplayName("Test NDVI 1.0-style with 'add(x,y)'")
    @Test
    public void testNdvi10AddXY() {
        testNdvi(createNormalizedDifferenceProcess10AddXY());
    }

    /**
     * NDVI implementation with "sum(data)", "subtract(x,y)" and "divide(x,y)" (API 1.0 style)
     */
    static OpenEOProcessScriptBuilder createNormalizedDifferenceProcess10SumData() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> empty = Collections.emptyMap();
        builder.expressionStart("divide", dummyMap("x", "y"));

        builder.argumentStart("x");
        builder.expressionStart("subtract", dummyMap("x", "y"));
        buildBandXYArguments(builder, 0, 1);
        builder.expressionEnd("subtract", dummyMap("x", "y"));
        builder.argumentEnd();

        builder.argumentStart("y");
        builder.expressionStart("sum", dummyMap("data"));
        buildBandArray(builder, "data");
        builder.expressionEnd("sum", dummyMap("data"));
        builder.argumentEnd();

        builder.expressionEnd("divide", dummyMap("x", "y"));
        return builder;
    }

    @DisplayName("Test NDVI 1.0-style with 'sum(data)'")
    @Test
    public void testNDVI10SumData() {
        testNdvi(createNormalizedDifferenceProcess10SumData());
    }

    @DisplayName("Test add constant")
    @Test
    public void testAddConstant() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> args = dummyMap("data");
        builder.expressionStart("sum", args);
        builder.arrayStart("data");
        builder.constantArrayElement(10);
        builder.constantArrayElement(20);
        builder.arrayEnd();
        builder.expressionEnd("sum", args);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile1 = fillByteArrayTile(3, 3, 9, 10, 11, 12);
        ByteArrayTile tile2 = fillByteArrayTile(3, 3, 5, 6, 7, 8);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1, tile2)));
        Tile res = result.apply(0);
        assertTileEquals(fillIntArrayTile(3, 3, 30, 30, 30, 30, 30, 30, 30, 30, 30), res);
    }

    @DisplayName("Test multiband XY constant")
    @Test
    public void testMultiBandMultiplyConstant() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> args = dummyMap("x", "y");
        String operator = "multiply";
        builder.expressionStart(operator, args);
        builder.argumentStart("x");
        builder.argumentEnd();
        builder.constantArgument("y", 10);
        builder.expressionEnd(operator, args);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile1 = fillByteArrayTile(3, 3, 9, 10, 11, 12);
        ByteArrayTile tile2 = fillByteArrayTile(3, 3, 5, 6, 7, 8);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1, tile2)));

        assertTileEquals(fillByteArrayTile(3, 3, 90, 100, 110, 120, 0, 0, 0, 0, 0), result.apply(0));
        assertTileEquals(fillByteArrayTile(3, 3, 50, 60, 70, 80, 0, 0, 0, 0, 0), result.apply(1));
    }

    @DisplayName("Test multiplying multiple tiles.")
    @Test
    public void testMultiBandMultiply() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> args = dummyMap("x", "y");
        String operator = "multiply";
        builder.expressionStart(operator, args);
        builder.argumentStart("x");
        builder.argumentEnd();
        builder.argumentStart("y");
        builder.argumentEnd();
        builder.expressionEnd(operator, args);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile1 = fillByteArrayTile(3, 3, 9, 10, 11, 12);
        ByteArrayTile tile2 = fillByteArrayTile(3, 3, 5, 6, 7, 8);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1, tile2)));

        assertTileEquals(fillByteArrayTile(3, 3, 81, 100, 121, 144, 0, 0, 0, 0, 0), result.apply(0));
        assertTileEquals(fillByteArrayTile(3, 3, 25, 36, 49, 64, 0, 0, 0, 0, 0), result.apply(1));

        assertEquals(FloatConstantNoDataCellType$.MODULE$, builder.getOutputCellType());
    }

    private void testLogicalComparisonWithConstant(String operator, int... expectedValues) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> args = dummyMap("x", "y");
        builder.expressionStart(operator, args);
        builder.argumentStart("x");
        builder.argumentEnd();
        builder.constantArgument("y", 10);
        builder.expressionEnd(operator, args);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();

        assertEquals(BitCellType$.MODULE$, builder.getOutputCellType());
        Tile tile = fillByteArrayTile(4, 3, 8, 9, 10, 11, 12);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile)));
        assertEquals(1, result.length());
        Tile res = result.apply(0);
        assertTileEquals(fillBitArrayTile(4, 3, expectedValues), res);
    }

    @DisplayName("Test logical 'eq' with constant")
    @Test
    public void testLogicalEqWithConstant() {
        testLogicalComparisonWithConstant("eq", 0, 0, 1, 0, 0);
    }

    @DisplayName("Test logical 'neq' with constant")
    @Test
    public void testLogicalNeqWithConstant() {
        testLogicalComparisonWithConstant("neq", 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    }

    @DisplayName("Test logical 'gt' with constant")
    @Test
    public void testLogicalGtWithConstant() {
        testLogicalComparisonWithConstant("gt", 0, 0, 0, 1, 1);
    }

    @DisplayName("Test logical 'gte' with constant")
    @Test
    public void testLogicalGteWithConstant() {
        testLogicalComparisonWithConstant("gte", 0, 0, 1, 1, 1);
    }

    @DisplayName("Test logical 'lt' with constant")
    @Test
    public void testLogicalLtWithConstant() {
        testLogicalComparisonWithConstant("lt", 1, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1);
    }

    @DisplayName("Test logical 'lte' with constant")
    @Test
    public void testLogicalLteWithConstant() {
        testLogicalComparisonWithConstant("lte", 1, 1, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1);
    }

    private void testLogicalComparisonXY(String operator, int... expectedValues) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> args = dummyMap("x", "y");
        builder.expressionStart(operator, args);
        buildBandXYArguments(builder, 0, 1);
        builder.expressionEnd(operator, args);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        Tile tile0 = fillByteArrayTile(3, 2, 8, 9, 10, 11, 12, 13);
        Tile tile1 = fillByteArrayTile(3, 2, 7, 9, 11, 9, 7, 5);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile1)));
        assertEquals(1, result.length());
        Tile res = result.apply(0);
        assertTileEquals(fillBitArrayTile(3, 2, expectedValues), res);
    }

    @DisplayName("Test logical 'eq' between bands")
    @Test
    public void testLogicalEqXY() {
        testLogicalComparisonXY("eq", 0, 1, 0, 0, 0, 0);
    }

    @DisplayName("Test logical 'neq' between bands")
    @Test
    public void testLogicalNeqXY() {
        testLogicalComparisonXY("neq", 1, 0, 1, 1, 1, 1);
    }

    @DisplayName("Test logical 'gt' between bands")
    @Test
    public void testLogicalGtXY() {
        testLogicalComparisonXY("gt", 1, 0, 0, 1, 1, 1);
    }

    @DisplayName("Test logical 'gte' between bands")
    @Test
    public void testLogicalGteXY() {
        testLogicalComparisonXY("gte", 1, 1, 0, 1, 1, 1);
    }

    @DisplayName("Test logical 'lt' between bands")
    @Test
    public void testLogicalLtXY() {
        testLogicalComparisonXY("lt", 0, 0, 1, 0, 0, 0);
    }

    @DisplayName("Test logical 'lte' between bands")
    @Test
    public void testLogicalLteXY() {
        testLogicalComparisonXY("lte", 0, 1, 1, 0, 0, 0);
    }


    @DisplayName("Test logical operations: 'not' after 'equals' (legacy)")
    @Test
    public void testLogicalNotEqWithExpression() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        builder.expressionStart("not", dummyMap("expression"));
        builder.argumentStart("expression");
        builder.expressionStart("eq", dummyMap("x", "y"));
        builder.argumentStart("x");
        builder.argumentEnd();
        builder.constantArgument("y", (byte) 10);
        builder.expressionEnd("eq", dummyMap("x", "y"));
        builder.argumentEnd();
        builder.expressionEnd("not", dummyMap("expression"));

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        Tile tile1 = fillByteArrayTile(4, 3, 8, 9, 10, 11, 12);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1)));
        Tile res = result.apply(0);
        assertTileEquals(fillBitArrayTile(4, 3, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1), res);
    }

    @DisplayName("Test logical operations: 'not' after 'equals'")
    @Test
    public void testLogicalNotEqWithX() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        builder.expressionStart("not", dummyMap("x"));
        builder.argumentStart("x");
        builder.expressionStart("eq", dummyMap("x", "y"));
        builder.argumentStart("x");
        builder.argumentEnd();
        builder.constantArgument("y", (byte) 10);
        builder.expressionEnd("eq", dummyMap("x", "y"));
        builder.argumentEnd();
        builder.expressionEnd("not", dummyMap("x"));

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        Tile tile1 = fillByteArrayTile(4, 3, 8, 9, 10, 11, 12);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1)));
        Tile res = result.apply(0);
        assertTileEquals(fillBitArrayTile(4, 3, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1), res);
    }




    @Test
    public void testLogicalBetweenBytes() {
        Object min = (byte) 9;
        Object max = (byte) 11;
        int[] expected = {0, 1, 1, 1, 0};
        testBetween(min, max, expected,null);
    }

    @Test
    public void testLogicalBetweenDoubles() {
        Object min = (Double) 10.;
        Object max = (Double) 14.;
        int[] expected = {0, 0, 1, 1, 1};
        testBetween(min, max, expected, false);
    }

    @Test
    public void testLogicalBetweenDoublesExclude() {
        Object min = (Double) 10.;
        Object max = (Double) 12.;
        int[] expected = {0, 0, 1, 1, 0};
        testBetween(min, max, expected, true);
    }

    private void testBetween(Object min, Object max, int[] expected, Object excludeMax) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> args = map4("x",null,"min", min, "max", max, "exclude_max",excludeMax);
        builder.expressionStart("between", args);
        builder.argumentStart("x");
        builder.argumentEnd();
        //builder.constantArgument("min", (byte) 9);
        //builder.constantArgument("max", (byte) 11);
        builder.expressionEnd("between", args);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        Tile tile1 = fillByteArrayTile(4, 3, 8, 9, 10, 11, 12);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1)));
        Tile res = result.apply(0);
        assertTileEquals(fillBitArrayTile(4, 3, expected), res);
    }

    private void testLogicalOperatorWithExpressionsArray(String operator, int... expectedValues) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> args = dummyMap("expressions");
        builder.expressionStart(operator, args);
        buildBandArray(builder, "expressions");
        builder.expressionEnd(operator, args);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();

        BitArrayTile x = fillBitArrayTile(4, 4, 0, 0, 1, 1);
        BitArrayTile y = fillBitArrayTile(4, 4, 0, 1, 0, 1);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(x, y)));
        assertEquals(1, result.length());
        Tile z = result.apply(0);
        assertEquals("bool", z.cellType().toString());
        assertTileEquals(fillBitArrayTile(4, 4, expectedValues), z);
    }

    @DisplayName("Test logical 'or' with 'expressions' (legacy)")
    @Test
    public void testLogicalOrWithExpressions() {
        testLogicalOperatorWithExpressionsArray("or", 0, 1, 1, 1);
    }

    @DisplayName("Test logical 'and' with 'expressions' (legacy)")
    @Test
    public void testLogicalAndWithExpressions() {
        testLogicalOperatorWithExpressionsArray("and", 0, 0, 0, 1);
    }

    @DisplayName("Test logical 'xor' with 'expressions' (legacy)")
    @Test
    public void testLogicalXorWithExpressions() {
        testLogicalOperatorWithExpressionsArray("xor", 0, 1, 1, 0);
    }

    private void testLogicalOperatorWithXY(String operator, int... expectedValues) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> args = new HashMap<String, Object>();
        args.put("x", "dummy");
        args.put("y", "dummy");
        builder.expressionStart(operator, args);
        buildBandXYArguments(builder, 0, 1);
        builder.expressionEnd(operator, args);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();

        BitArrayTile x = fillBitArrayTile(4, 4, 0, 0, 1, 1);
        BitArrayTile y = fillBitArrayTile(4, 4, 0, 1, 0, 1);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(x, y)));
        assertEquals(1, result.length());
        Tile z = result.apply(0);
        assertEquals("bool", z.cellType().toString());
        assertTileEquals(fillBitArrayTile(4, 4, expectedValues), z);
    }

    @DisplayName("Test logical 'or' with 'x' and 'y'")
    @Test
    public void testLogicalOrWithXY() {
        testLogicalOperatorWithXY("or", 0, 1, 1, 1);
    }

    @DisplayName("Test logical 'and' with 'x' and 'y'")
    @Test
    public void testLogicalAndWithXY() {
        testLogicalOperatorWithXY("and", 0, 0, 0, 1);
    }

    @DisplayName("Test logical 'xor' with 'x' and 'y'")
    @Test
    public void testLogicalXorWithXY() {
        testLogicalOperatorWithXY("xor", 0, 1, 1, 0);
    }

    private void testMathXY(String operator, int... expectedValues) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        builder.expressionStart(operator, dummyMap("x", "y"));
        buildBandXYArguments(builder, 0, 1);
        builder.expressionEnd(operator, dummyMap("x", "y"));

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        Tile tile0 = fillIntArrayTile(3, 2, 3, 4, 5, 6, 7, 8);
        Tile tile1 = fillIntArrayTile(3, 2, 1, 2, 4, 8, 16, 20);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile1)));
        assertEquals(1, result.length());
        Tile res = result.apply(0);
        IntArrayTile expectedTile = fillIntArrayTile(3, 2, expectedValues);
        assertTileEquals(expectedTile, res);

        Tile doubleResult = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0.convert(CellType.fromName("float64")), tile1.convert(CellType.fromName("float64"))))).apply(0);
        assertTileEquals(expectedTile.convert(CellType.fromName("float64")), doubleResult);
    }

    @DisplayName("Test math 'add(x,y)'")
    @Test
    public void testMathAddXY() {
        testMathXY("add", 4, 6, 9, 14, 23, 28);
    }

    @DisplayName("Test math 'add(x,y)' bit cells")
    @Test
    public void testAddWithBitCells() {
        testMathWithBitCells("add",1,2,1,0);
    }

    @DisplayName("Test math 'subtract(x,y)' bit cells")
    @Test
    public void testSubtractWithBitCells() {
        testMathWithBitCells("subtract",-1,0,1,0);
    }

    public void testMathWithBitCells(String operator, int... expectedValues) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        builder.expressionStart(operator, dummyMap("x", "y"));
        buildBandXYArguments(builder, 0, 1);
        builder.expressionEnd(operator, dummyMap("x", "y"));

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        Tile tile0 = fillBitArrayTile(3, 2, 0,1,1,0);
        Tile tile1 = fillBitArrayTile(3, 2, 1,1,0,0);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile1)));
        assertEquals(1, result.length());
        Tile res = result.apply(0);
        ArrayTile expectedTile = fillByteArrayTile(3, 2, expectedValues).convert(new ByteUserDefinedNoDataCellType(((Integer)127).byteValue()));
        assertTileEquals(expectedTile, res);


    }

    @DisplayName("Test math 'subtract(x,y)'")
    @Test
    public void testMathSubtractXY() {
        testMathXY("subtract", 2, 2, 1, -2, -9, -12);
    }

    @DisplayName("Test math 'multiply(x,y)'")
    @Test
    public void testMultiplyXY() {
        testMathXY("multiply", 3, 8, 20, 48, 112, 160);
    }

    @DisplayName("Test math 'divide(x,y)'")
    @Test
    public void testDivideXY() {
        testMathXY("divide", 3, 2, 1, 0, 0, 0);
    }

    @DisplayName("Test math 'normalized_difference(x,y)'")
    @Test
    public void testNormalizedDifferenceXY() {
        String operator = "normalized_difference";
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        builder.expressionStart(operator, dummyMap("x", "y"));
        buildBandXYArguments(builder, 0, 1);
        builder.expressionEnd(operator, dummyMap("x", "y"));

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        DoubleArrayTile tile1 = fillDoubleArrayTile(4, 2, 3, 10, 6, 3, 9, 15, 0, Double.NaN);
        DoubleArrayTile tile2 = fillDoubleArrayTile(4, 2, 0, 6, 10, 9, 7, 17, 0, Double.NaN);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1, tile2)));
        assertEquals(1, result.length());
        Tile ndvi = result.apply(0);
        assertDoubleTileEquals(fillDoubleArrayTile(4, 2, 1.0, 0.25, -0.25, -0.5, 0.125, -0.0625, Double.NaN, Double.NaN), ndvi);
    }


    private void testMathData(String operator, int... expectedValues) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        builder.expressionStart(operator, dummyMap("data"));
        buildBandArray(builder, "data");
        builder.expressionEnd(operator, dummyMap("data"));

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        Tile tile0 = fillIntArrayTile(3, 2, 3, 4, 5, 6, 7, 8);
        Tile tile1 = fillIntArrayTile(3, 2, 1, 2, 4, 8, 16, 20);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile1)));
        assertEquals(1, result.length());
        Tile res = result.apply(0);
        assertTileEquals(fillIntArrayTile(3, 2, expectedValues), res);
    }

    @DisplayName("Test math 'sum(data)'")
    @Test
    public void testMathSumData() {
        testMathData("sum", 4, 6, 9, 14, 23, 28);
    }

    @DisplayName("Test math 'subtract(data)'")
    @Test
    public void testMathSubtractData() {
        testMathData("subtract", 2, 2, 1, -2, -9, -12);
    }

    @DisplayName("Test math 'multiply(data)'")
    @Test
    public void testMultiplyData() {
        testMathData("multiply", 3, 8, 20, 48, 112, 160);
    }

    @DisplayName("Test math 'product(data)'")
    @Test
    public void testProductData() {
        testMathData("product", 3, 8, 20, 48, 112, 160);
    }

    @DisplayName("Test math 'divide(data)'")
    @Test
    public void testDivideData() {
        testMathData("divide", 3, 2, 1, 0, 0, 0);
    }


    @DisplayName("Test proper error handling in case argument is missing.")
    @Test
    public void testException() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        builder.expressionStart("not", dummyMap("x"));
        builder.argumentStart("x");
        builder.expressionStart("eq", dummyMap("x", "y"));
        //builder.argumentStart("x");
        //builder.argumentEnd();
        builder.constantArgument("y",(byte)10);

        assertThrows(IllegalArgumentException.class,() -> {
            builder.expressionEnd("eq", dummyMap("x", "y"));
        });
    }

    @DisplayName("Test if process")
    @Test
    public void testIf() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = dummyMap("accept","reject");
        builder.expressionStart("if", arguments);

        builder.argumentStart("value");
        buildArrayElementProcess(builder, 1);
        builder.argumentEnd();
        builder.argumentStart("accept");
        builder.argumentEnd();

        builder.expressionEnd("if",arguments);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile0 = ByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);
        byte nodataVal = ByteConstantNoDataArrayTile.empty(1, 1).array()[0];

        ByteArrayTile value = ByteConstantNoDataArrayTile.fill((byte) 1, 4, 4);
        value.set(0, 0, 0);
        value.set(1, 0, 0);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0, value)));
        Tile res = result.apply(0);
        tile0.set(0,0,nodataVal);
        tile0.set(1,0,nodataVal);
        assertTileEquals(tile0, res);
    }

    @DisplayName("Test if process with a reject")
    @Test
    public void testIfWithReject() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = dummyMap("accept","reject");
        builder.expressionStart("if", arguments);

        builder.argumentStart("value");

        Map<String, Object> args = new HashMap<>();
        args.put("y", 7.0);
        args.put("x", Collections.singletonMap("from_parameter","x"));
        builder.expressionStart("gt", args);
        builder.constantArgument("y", 7.0);
        builder.argumentStart("x");
        builder.argumentEnd();
        builder.expressionEnd("gt", args);
        builder.argumentEnd();
        builder.argumentStart("accept");
        builder.fromParameter("x");
        builder.argumentEnd();
        builder.constantArgument("reject",1.5f);

        builder.expressionEnd("if",arguments);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        FloatArrayTile tile0 = FloatArrayTile.fill( 10.5f, 4, 4);

        tile0.setDouble(2, 0, Float.NaN);
        tile0.setDouble(1, 0, 5.5);
        tile0.setDouble(1, 1, 5.5);

        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0)));
        Tile res = result.apply(0);
        tile0.setDouble(2,0,1.5);
        tile0.setDouble(1,0,1.5);
        tile0.setDouble(1,1,1.5);
        assertDoubleTileEquals(tile0,res);
    }


    @DisplayName("Test if process with a reject")
    @Test
    public void testIfWithRejectCube() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = dummyMap("accept","reject");
        builder.expressionStart("if", arguments);

        builder.argumentStart("value");

        Map<String, Object> args = new HashMap<>();
        args.put("y", 7.0);
        args.put("x", Collections.singletonMap("from_parameter","x"));
        builder.expressionStart("gt", args);
        builder.constantArgument("y", 7.0);
        builder.argumentStart("x");
        builder.argumentEnd();
        builder.expressionEnd("gt", args);
        builder.argumentEnd();
        builder.argumentStart("reject");
        builder.fromParameter("x");
        builder.argumentEnd();
        builder.constantArgument("accept",1.5f);

        builder.expressionEnd("if",arguments);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        FloatArrayTile tile0 = FloatArrayTile.fill( 10.5f, 4, 4);

        tile0.setDouble(2, 0, Float.NaN);
        tile0.setDouble(1, 0, 5.5);
        tile0.setDouble(1, 1, 5.5);

        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0,tile0)));
        FloatArrayTile expected = FloatArrayTile.fill( 1.5f, 4, 4);
        expected.setDouble(2,0,Float.NaN);
        expected.setDouble(1,0,5.5);
        expected.setDouble(1,1,5.5);
        assertDoubleTileEquals(expected,result.apply(0));
    }

    @DisplayName("Test first process with nodata values")
    @Test
    public void testFirstWithNoData() {
        // Assume we make a first(data, ignore_nodata=false) request on a list of tiles with a time dimension.
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = Collections.singletonMap("ignore_nodata", false);
        builder.expressionStart("first", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();
        builder.constantArgument("ignore_nodata", false);

        builder.expressionEnd("first",arguments);
        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();

        // When we have a data tile for every timestep.
        ByteArrayTile tile_timestep0 = ByteConstantNoDataArrayTile.empty(4,4);
        tile_timestep0.set(2,2,3);
        ByteArrayTile tile_timestep1 = ByteConstantNoDataArrayTile.fill((byte)5, 4, 4);

        // Then selecting all first pixels from a list of one timestep just returns us that tile.
        Seq<Tile> single_input = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile_timestep0.mutable().copy())));
        assertTileEquals(tile_timestep0, single_input.apply(0));

        // When a second timestep is added that has actual values, the Nodata from the first tile will still be selected.
        // Because ignore_nodata is set to false.
        Seq<Tile> multiple_input = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile_timestep0.mutable().copy(), tile_timestep1.mutable().copy())));
        assertEquals(Int.MinValue(), multiple_input.apply(0).get(0,0));
        // Including the one non-NoData value in timestep 0.
        assertEquals(3, multiple_input.apply(0).get(2,2));

        // Make sure that the celltypes have not changed to double like they would for processes such as median/mean.
        assertEquals(tile_timestep0.cellType(), single_input.apply(0).cellType());
        assertEquals(tile_timestep0.cellType(), multiple_input.apply(0).cellType());
    }

    @DisplayName("Test first process without nodata values")
    @Test
    public void testFirstIgnoreNodata() {
        // Assume we make a first(data, ignore_nodata=true) request on a list of tiles with a time dimension.
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = Collections.singletonMap("ignore_nodata", true);
        builder.expressionStart("first", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();
        builder.constantArgument("ignore_nodata", true);

        builder.expressionEnd("first",arguments);
        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();

        // When we have a data tile for every timestep.
        ByteArrayTile tile_timestep0 = ByteConstantNoDataArrayTile.empty(4,4);
        tile_timestep0.set(2,2,3);
        ByteArrayTile tile_timestep1 = ByteConstantNoDataArrayTile.fill((byte)5, 4, 4);

        // Then selecting all first pixels from a list of one timestep just returns us that tile, even if it has NoData values.
        Seq<Tile> single_input = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile_timestep0.mutable().copy())));
        assertTileEquals(tile_timestep0, single_input.apply(0));

        // When a second timestep is added that has actual values, those will be selected as first instead of the NoData values.
        Seq<Tile> multiple_input = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile_timestep0.mutable().copy(), tile_timestep1.mutable().copy())));
        assertEquals(5, multiple_input.apply(0).get(0,0));
        // Except for the one non-NoData value in timestep 0.
        assertEquals(3, multiple_input.apply(0).get(2,2));

        // Make sure that the celltypes have not changed to double like they would for processes such as median/mean.
        assertEquals(tile_timestep0.cellType(), single_input.apply(0).cellType());
        assertEquals(tile_timestep0.cellType(), multiple_input.apply(0).cellType());
    }

    @DisplayName("Test last process with nodata values")
    @Test
    public void testLastWithNoData() {
        // Assume we make a last(data, ignore_nodata=false) request on a list of tiles with a time dimension.
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = Collections.singletonMap("ignore_nodata", false);
        builder.expressionStart("last", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();
        builder.constantArgument("ignore_nodata", false);

        builder.expressionEnd("last",arguments);
        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();

        // When we have a data tile for every timestep.
        ByteArrayTile tile_timestep0 = ByteConstantNoDataArrayTile.fill((byte)5, 4, 4);
        ByteArrayTile tile_timestep1 = ByteConstantNoDataArrayTile.empty(4,4);
        tile_timestep1.set(2,2,3);

        // Then selecting all last pixels from a list of one timestep just returns us that tile.
        Seq<Tile> single_input = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile_timestep1.mutable().copy())));
        assertTileEquals(tile_timestep1, single_input.apply(0));

        // When a second timestep is prepended that has actual values, the Nodata from the last tile will be selected.
        // Because ignore_nodata is set to false.
        Seq<Tile> multiple_input = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile_timestep0.mutable().copy(), tile_timestep1.mutable().copy())));
        assertEquals(Int.MinValue(), multiple_input.apply(0).get(0,0));
        // Including the one non-NoData value in timestep 1.
        assertEquals(3, multiple_input.apply(0).get(2,2));

        // Make sure that the celltypes have not changed to double like they would for processes such as median/mean.
        assertEquals(tile_timestep0.cellType(), single_input.apply(0).cellType());
        assertEquals(tile_timestep0.cellType(), multiple_input.apply(0).cellType());
    }

    @DisplayName("Test last process without nodata values")
    @Test
    public void testLastIgnoreNodata() {
        // Assume we make a last(data, ignore_nodata=true) request on a list of tiles with a time dimension.
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = Collections.singletonMap("ignore_nodata", true);
        builder.expressionStart("last", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();
        builder.constantArgument("ignore_nodata", true);

        builder.expressionEnd("last",arguments);
        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();

        // When we have a data tile for every timestep.
        ByteArrayTile tile_timestep0 = ByteConstantNoDataArrayTile.fill((byte)5, 4, 4);
        ByteArrayTile tile_timestep1 = ByteConstantNoDataArrayTile.empty(4,4);
        tile_timestep1.set(2,2,3);

        // Then selecting all last pixels from a list of one timestep just returns us that tile.
        Seq<Tile> single_input = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile_timestep1.mutable().copy())));
        assertTileEquals(tile_timestep1, single_input.apply(0));

        // When a second timestep is prepended that has actual values, those will be selected as last instead of the NoData values.
        Seq<Tile> multiple_input = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile_timestep0.mutable().copy(), tile_timestep1.mutable().copy())));
        assertEquals(5, multiple_input.apply(0).get(0,0));
        // Except for the one non-NoData value in timestep 1.
        assertEquals(3, multiple_input.apply(0).get(2,2));

        // Make sure that the celltypes have not changed to double like they would for processes such as median/mean.
        assertEquals(tile_timestep0.cellType(), single_input.apply(0).cellType());
        assertEquals(tile_timestep0.cellType(), multiple_input.apply(0).cellType());
    }

    @DisplayName("Test exp process")
    @Test
    public void testExp() {
        testUnary( "exp", "p",1, 33.0, 0.0, 0.0,Double.NaN,1.0,1.0,1.0,1.0);
    }

    @DisplayName("Test int process")
    @Test
    public void testInt() {
        testUnary( "int", "x",0.0, 3.0, 0.0, -3.0,Double.NaN,0.0,0.0,0.0,0.0);
    }

    private void testUnary( String processName, String argName, double... expectedValues) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = Collections.emptyMap();
        builder.expressionStart(processName, arguments);
        builder.argumentStart(argName);
        builder.argumentEnd();
        builder.expressionEnd(processName,arguments);
        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();

        Seq<Tile> result1 = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(fillFloatArrayTile(3, 3, 0, 3.5, -0.4, -3.5, Double.NaN))));

        assertTileEquals(fillFloatArrayTile(3, 3, expectedValues), result1.head());

    }

    @DisplayName("Test 'is_nodata' and 'is_nan' processes")
    @Test
    public void testIsNoDataAndIsNan() {
        // IsNoData
        OpenEOProcessScriptBuilder isNoDataBuilder = new OpenEOProcessScriptBuilder();
        isNoDataBuilder.expressionStart("is_nodata", dummyMap("x"));
        isNoDataBuilder.argumentStart("x");
        isNoDataBuilder.argumentEnd();
        isNoDataBuilder.expressionEnd("is_nodata", dummyMap("x"));

        Function1<Seq<Tile>, Seq<Tile>> transformation = isNoDataBuilder.generateFunction();
        FloatArrayTile tile0 = FloatConstantNoDataArrayTile.empty(3, 3);
        tile0.setDouble(0,0, 5.0);
        tile0.setDouble(2,1, 4.0);
        tile0.setDouble(2,2, 17.0);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile0)));
        Tile res = result.apply(0);
        int expected_values[] = {0, 1, 1, 1, 1, 0, 1, 1, 0};

        assertTileEquals(fillBitArrayTile(3, 3, expected_values), res);

        // IsNan
        OpenEOProcessScriptBuilder isNanBuilder = new OpenEOProcessScriptBuilder();
        isNanBuilder.expressionStart("is_nan", dummyMap("x"));
        isNanBuilder.argumentStart("x");
        isNanBuilder.argumentEnd();
        isNanBuilder.expressionEnd("is_nan", dummyMap("x"));
        Seq<Tile> result2 = isNanBuilder.generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile0)));

        assertTileEquals(fillBitArrayTile(3, 3, expected_values), result2.head());
    }

    @DisplayName("Test array_element process")
    @Test
    public void testArrayElement() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = Collections.singletonMap("index",1);
        builder.expressionStart("array_element", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();
        builder.argumentStart("index");
        builder.argumentEnd();

        builder.expressionEnd("array_element",arguments);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile0 = ByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);
        ByteArrayTile tile1 = ByteConstantNoDataArrayTile.fill((byte) 5, 4, 4);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile1)));
        Tile res = result.apply(0);
        assertTileEquals(tile1, res);
    }

    @DisplayName("Test array_element by label process")
    @Test
    public void testArrayElementByLabel() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = Collections.singletonMap("label","B03");
        builder.expressionStart("array_element", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();
        builder.argumentStart("label");
        builder.argumentEnd();

        builder.expressionEnd("array_element",arguments);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction(Collections.singletonMap("array_labels",Arrays.asList("B02","B03")));
        ByteArrayTile tile0 = ByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);
        ByteArrayTile tile1 = ByteConstantNoDataArrayTile.fill((byte) 5, 4, 4);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile1)));
        Tile res = result.apply(0);
        assertTileEquals(tile1, res);
    }

    @DisplayName("Test array_find process")
    @Test
    public void testArrayFind() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = Collections.emptyMap();
        builder.expressionStart("array_find", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();
        builder.argumentStart("value");
        Map<String, Object> valueArgs = Collections.singletonMap("index",1);
        builder.expressionStart("array_element", valueArgs);

        builder.argumentStart("data");
        builder.argumentEnd();
        builder.argumentStart("index");
        builder.argumentEnd();

        builder.expressionEnd("array_element",valueArgs);
        builder.argumentEnd();

        builder.expressionEnd("array_find",arguments);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile0 = ByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);
        ByteArrayTile tile1 = ByteConstantNoDataArrayTile.fill((byte) 5, 4, 4);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile1)));
        ByteArrayTile expectedResult = ByteConstantNoDataArrayTile.fill((byte) 1, 4, 4);
        assertTileEquals(expectedResult, result.head());


    }

    @DisplayName("Test array_find process reverse")
    @Test
    public void testArrayFindReverse() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = Collections.singletonMap("reverse",true);
        builder.expressionStart("array_find", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();
        builder.argumentStart("value");
        Map<String, Object> valueArgs = Collections.singletonMap("index",1);
        builder.expressionStart("array_element", valueArgs);

        builder.argumentStart("data");
        builder.argumentEnd();
        builder.argumentStart("index");
        builder.argumentEnd();

        builder.expressionEnd("array_element",valueArgs);
        builder.argumentEnd();
        builder.constantArgument("reverse",true);

        builder.expressionEnd("array_find",arguments);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile0 = ByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);
        ByteArrayTile tile1 = ByteConstantNoDataArrayTile.fill((byte) 5, 4, 4);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile1,tile1)));
        ByteArrayTile expectedResult = ByteConstantNoDataArrayTile.fill((byte) 2, 4, 4);
        assertTileEquals(expectedResult, result.head());


    }

    @DisplayName("Test array_find process no match")
    @Test
    public void testArrayFindNoMatch() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = Collections.emptyMap();
        builder.expressionStart("array_find", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();
        builder.constantArgument("value",100);

        builder.expressionEnd("array_find",arguments);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile0 = ByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);
        ByteArrayTile tile1 = ByteConstantNoDataArrayTile.fill((byte) 5, 4, 4);


        Tile emptyResult = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile1))).head();
        assertTrue(emptyResult.isNoDataTile());
    }

    @DisplayName("Test array_modify process: insert")
    @Test
    public void testArrayModifyInsert() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = Collections.singletonMap("index",1);
        builder.expressionStart("array_modify", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();
        builder.argumentStart("values");
        buildArrayElementProcess(builder,0);
        builder.argumentEnd();
        builder.argumentStart("index");
        builder.constantArrayElement(1);
        builder.argumentEnd();

        builder.expressionEnd("array_modify",arguments);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile0 = ByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);
        ByteArrayTile tile1 = ByteConstantNoDataArrayTile.fill((byte) 5, 4, 4);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile1)));
        assertEquals(3,result.size());
        Tile res = result.apply(0);
        assertTileEquals(tile0, res);
        assertTileEquals(tile0, result.apply(1));
        assertTileEquals(tile1, result.apply(2));
    }

    @DisplayName("Test array_concat")
    @Test
    public void testArrayConcat() {
        /* Builder setup based on:
         python openeogeotrellis/geotrellis_tile_processgraph_visitor.py '{
                "band0": {"process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 0}},
                "band1": {"process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 1}},
                "band2": {"process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 2}},
                "band3": {"process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 3}},
                "band4": {"process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 4}},
                "arrayconcat": {
                    "process_id": "array_concat",
                    "arguments": {
                        "array1": [{"from_node": "band4"}, {"from_node": "band2"}, {"from_node": "band0"}],
                        "array2": [{"from_node": "band1"}, {"from_node": "band3"}]
                    },
                    "result": true
                }
            }'
         */

        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        builder.expressionStart("array_concat", dummyMap("array1", "array2"));
        builder.arrayStart("array1");
        builder.expressionStart("array_element", map2("data","dummy", "index", 4));
        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();
        builder.constantArgument("index", 4);
        builder.expressionEnd("array_element", map2("data","dummy","index", 4));
        builder.arrayElementDone();
        builder.expressionStart("array_element", map2("data","dummy","index", 2));
        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();
        builder.constantArgument("index", 2);
        builder.expressionEnd("array_element", map2("data","dummy","index", 2));
        builder.arrayElementDone();
        builder.expressionStart("array_element", map2("data","dummy","index", 0));
        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();
        builder.constantArgument("index", 0);
        builder.expressionEnd("array_element", map2("data","dummy","index", 0));
        builder.arrayElementDone();
        builder.arrayEnd();
        builder.arrayStart("array2");
        builder.expressionStart("array_element", map2("data","dummy","index", 1));
        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();
        builder.constantArgument("index", 1);
        builder.expressionEnd("array_element", map2("data","dummy","index", 1));
        builder.arrayElementDone();
        builder.expressionStart("array_element", map2("data","dummy","index", 3));
        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();
        builder.constantArgument("index", 3);
        builder.expressionEnd("array_element", map2("data","dummy","index", 3));
        builder.arrayElementDone();
        builder.arrayEnd();
        builder.expressionEnd("array_concat", dummyMap("array1", "array2"));

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile t0 = ByteConstantNoDataArrayTile.fill((byte) 0, 4, 4);
        ByteArrayTile t1 = ByteConstantNoDataArrayTile.fill((byte) 1, 4, 4);
        ByteArrayTile t2 = ByteConstantNoDataArrayTile.fill((byte) 2, 4, 4);
        ByteArrayTile t3 = ByteConstantNoDataArrayTile.fill((byte) 3, 4, 4);
        ByteArrayTile t4 = ByteConstantNoDataArrayTile.fill((byte) 4, 4, 4);
        ByteArrayTile t5 = ByteConstantNoDataArrayTile.fill((byte) 5, 4, 4);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(t0, t1, t2, t3, t4, t5)));
        assertEquals(5, result.size());
        assertTileEquals(t4, result.apply(0));
        assertTileEquals(t2, result.apply(1));
        assertTileEquals(t0, result.apply(2));
        assertTileEquals(t1, result.apply(3));
        assertTileEquals(t3, result.apply(4));
    }

    @DisplayName("Test array_create")
    @Test
    public void testArrayCreate() {
        /* Builder setup based on:
         python openeogeotrellis/geotrellis_tile_processgraph_visitor.py '{
                "band0": {"process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 0}},
                "band1": {"process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 1}},
                "arrayconcat": {
                    "process_id": "array_create",
                    "arguments": {
                        "data": [{"from_node": "band1"}, {"from_node": "band0"}, {"from_node": "band1"}],
                        "repeat": 2
                    },
                    "result": true
                }
            }'
         */

        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        builder.expressionStart("array_create", map2("data", "dummy", "repeat", 2));
        builder.arrayStart("data");
        builder.expressionStart("array_element", map2("data","dummy", "index", 1));
        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();
        builder.constantArgument("index", 1);
        builder.expressionEnd("array_element", map2("data","dummy", "index", 1));
        builder.arrayElementDone();
        builder.expressionStart("array_element", map2("data","dummy", "index", 0));
        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();
        builder.constantArgument("index", 0);
        builder.expressionEnd("array_element", map2("data","dummy", "index", 0));
        builder.arrayElementDone();
        builder.expressionStart("array_element", map2("data","dummy", "index", 1));
        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();
        builder.constantArgument("index", 1);
        builder.expressionEnd("array_element", map2("data","dummy", "index", 1));
        builder.arrayElementDone();
        builder.arrayEnd();
        builder.constantArgument("repeat", 2);
        builder.expressionEnd("array_create", map2("data", "dummy", "repeat", 2));

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile t0 = ByteConstantNoDataArrayTile.fill((byte) 0, 4, 4);
        ByteArrayTile t1 = ByteConstantNoDataArrayTile.fill((byte) 1, 4, 4);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(t0, t1)));
        assertEquals(6, result.size());
        assertTileEquals(t1, result.apply(0));
        assertTileEquals(t0, result.apply(1));
        assertTileEquals(t1, result.apply(2));
        assertTileEquals(t1, result.apply(3));
        assertTileEquals(t0, result.apply(4));
        assertTileEquals(t1, result.apply(5));
    }

    @DisplayName("Test array_interpolate_linear process")
    @Test
    public void testArrayInterpolateLinear() {
        OpenEOProcessScriptBuilder builder = createArrayInterpolateLinear();

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile0 = ByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);
        ByteArrayTile nodataTile = ByteConstantNoDataArrayTile.empty(4, 4);
        ByteArrayTile tile1 = ByteConstantNoDataArrayTile.fill((byte) 5, 4, 4);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(nodataTile,nodataTile,tile0, nodataTile,tile1, nodataTile,nodataTile,tile0,nodataTile).stream().map(byteArrayTile -> byteArrayTile.copy()).collect(Collectors.toList())));
        assertEquals(9,result.size());
        assertTrue(result.apply(0).isNoDataTile());
        assertTrue(result.apply(1).isNoDataTile());
        assertEquals(10,result.apply(2).get(0,0));
        assertEquals(7,result.apply(3).get(0,0));
        assertEquals(5,result.apply(4).get(0,0));
        assertEquals(6,result.apply(5).get(0,0));
        assertEquals(8,result.apply(6).get(0,0));
        assertEquals(10,result.apply(7).get(0,0));
        assertTrue(result.apply(8).isNoDataTile());

    }

    @DisplayName("Test linear_scale_range process")
    @Test
    public void testLinearScaleRange() {
        OpenEOProcessScriptBuilder builder = createLinearScaleRange(0,2,0,240);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        Tile tile0 = FloatConstantNoDataArrayTile.fill(1, 4, 4);
        Tile tile1 = FloatConstantNoDataArrayTile.fill(3, 4, 4);
        Tile tile2 = FloatConstantNoDataArrayTile.fill(-1, 4, 4);
        Tile tile3 = FloatConstantNoDataArrayTile.fill(1.9f, 4, 4);
        Tile nodataTile = ByteConstantNoDataArrayTile.empty(4, 4);

        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(nodataTile,tile0,tile1,tile2,tile3)));

        assertTrue(result.apply(0).isNoDataTile());

        assertEquals(120,result.apply(1).get(0,0));
        assertEquals(240,result.apply(2).get(0,0));
        assertEquals(0,result.apply(3).get(0,0));
        assertEquals(228,result.apply(4).get(0,0));
        assertEquals(UByteUserDefinedNoDataCellType.apply((byte)255),result.apply(1).cellType());


    }

    @DisplayName("Test linear_scale_range process with conversion to short")
    @Test
    public void testLinearScaleRangeToShort() {
        OpenEOProcessScriptBuilder builder = createLinearScaleRange(0,10,0,1000);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        Tile tile0 = FloatConstantNoDataArrayTile.fill(1, 4, 4);
        Tile tile1 = FloatConstantNoDataArrayTile.fill(3, 4, 4);
        Tile tile2 = FloatConstantNoDataArrayTile.fill(-10, 4, 4);
        Tile tile3 = FloatConstantNoDataArrayTile.fill(19f, 4, 4);
        Tile nodataTile = ByteConstantNoDataArrayTile.empty(4, 4);

        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(nodataTile,tile0,tile1,tile2,tile3)));

        assertTrue(result.apply(0).isNoDataTile());
        assertEquals(UShortUserDefinedNoDataCellType.apply((short)65535),result.apply(1).cellType());

        assertEquals(100,result.apply(1).get(0,0));
        assertEquals(300,result.apply(2).get(0,0));
        assertEquals(0,result.apply(3).get(0,0));
        assertEquals(1000,result.apply(4).get(0,0));


    }

    @DisplayName("Test median process")
    @Test
    public void testMedian() {

        Tile tile0 = ByteConstantNoDataArrayTile.fill((byte)1, 4, 4);
        Tile tile1 = ByteConstantNoDataArrayTile.fill((byte)3, 4, 4);
        Tile tile2 = ByteConstantNoDataArrayTile.fill((byte)-10, 4, 4);
        Tile tile3 = ByteConstantNoDataArrayTile.fill((byte)19, 4, 4);
        Tile nodataTile = ByteConstantNoDataArrayTile.empty(4, 4);

        Seq<Tile> result = createMedian(null).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(nodataTile.mutable().copy(),tile1.mutable().copy(),nodataTile,tile1,tile1,tile2,nodataTile,tile3,tile0)));
        assertEquals(ByteConstantNoDataCellType.withDefaultNoData(),result.apply(0).cellType());

        assertEquals(3,result.apply(0).get(0,0));

        Seq<Tile> result_nodata = createMedian(false).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1.mutable().copy(),tile1.mutable().copy(),tile1,tile2,nodataTile,tile3,tile0)));
        assertTrue(result_nodata.apply(0).isNoDataTile());

        Seq<Tile> single_input = createMedian(true).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(tile2.mutable().copy())));
        assertEquals(-10,single_input.apply(0).get(0,0));

        Seq<Tile> even_input = createMedian(true).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(tile2.mutable().copy(),tile1)));
        assertEquals(-3.0,even_input.apply(0).get(0,0));
    }

    @DisplayName("Test sd process")
    @Test
    public void testStandardDeviation() {
        Tile tile0 = FloatConstantNoDataArrayTile.fill((byte)1.0, 4, 4);
        Tile tile1 = FloatConstantNoDataArrayTile.fill((byte)3.0, 4, 4);
        Tile tile2 = FloatConstantNoDataArrayTile.fill((byte)-10.0, 4, 4);
        Tile tile3 = FloatConstantNoDataArrayTile.fill((byte)19.0, 4, 4);
        Tile nodataTile = FloatConstantNoDataArrayTile.empty(4, 4);

        Seq<Tile> result = createStandardDeviation(null).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(nodataTile.mutable().copy(),tile1.mutable().copy(),nodataTile,tile1,tile1,tile2,nodataTile,tile3,tile0)));
        assertEquals(FloatConstantNoDataArrayTile.empty(0, 0).cellType(), result.apply(0).cellType());

        assertEquals(9.261029243469238,result.apply(0).getDouble(0,0));

        Seq<Tile> result_nodata = createStandardDeviation(false).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1.mutable().copy(),tile1.mutable().copy(),tile1,tile2,nodataTile,tile3,tile0)));
        assertTrue(result_nodata.apply(0).isNoDataTile());

        Seq<Tile> input1 = createStandardDeviation(true).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(tile2.mutable().copy(), tile3)));
        assertEquals(20.50609588623047, input1.apply(0).getDouble(0,0));

        Seq<Tile> input2 = createStandardDeviation(true).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(tile2.mutable().copy(),tile1, nodataTile)));
        assertEquals(9.192388534545898, input2.apply(0).getDouble(0,0));
    }

    @DisplayName("Test quantiles process")
    @Test
    public void testQuantiles() {

        Tile tile0 = ByteConstantNoDataArrayTile.fill((byte)1, 4, 4);
        Tile tile1 = ByteConstantNoDataArrayTile.fill((byte)3, 4, 4);
        Tile tile2 = ByteConstantNoDataArrayTile.fill((byte)-10, 4, 4);
        Tile tile3 = ByteConstantNoDataArrayTile.fill((byte)19, 4, 4);
        Tile nodataTile = ByteConstantNoDataArrayTile.empty(4, 4);

        Seq<Tile> result = createQuantiles(null,2).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(nodataTile.mutable().copy(),tile1.mutable().copy(),nodataTile,tile1,tile1,tile2,nodataTile,tile3,tile0)));
        assertEquals(ByteConstantNoDataCellType.withDefaultNoData(),result.apply(0).cellType());

        assertEquals(3,result.apply(0).get(0,0));

        //Seq<Tile> result_nodata = createQuantiles(false,2).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1.mutable().copy(),tile1.mutable().copy(),tile1,tile2,nodataTile,tile3,tile0)));
        //assertTrue(result_nodata.apply(0).isNoDataTile());

        Seq<Tile> single_input = createQuantiles(true,2).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(tile2)));
        assertEquals(-10,single_input.apply(0).get(0,0));

        Seq<Tile> even_input = createQuantiles(true,2).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(tile2,tile1)));
        assertEquals(-3.0,even_input.apply(0).get(0,0));

        Seq<Tile> quartiles = createQuantiles(null,4).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(nodataTile,tile1,nodataTile,tile1,tile1,tile2,nodataTile,tile3,tile0)));
        Object[] elements = JavaConverters.seqAsJavaListConverter(quartiles).asJava().stream().map(v1 -> v1.get(0, 0)).toArray();
        //nd,3,nd,3,3,-10,nd,19,nd
        // -10,1 ,3 3 3 19 nd nd nd nd

        assertArrayEquals(new Object[]{-1,3,7}, elements);
    }

    @DisplayName("Test clip process")
    @Test
    public void testClip() {
        OpenEOProcessScriptBuilder builder1 = createClip(1.0, 2);
        OpenEOProcessScriptBuilder builder2 = createClip(1.5, 2.5);

        Function1<Seq<Tile>, Seq<Tile>> transformation1 = builder1.generateFunction();
        Function1<Seq<Tile>, Seq<Tile>> transformation2 = builder2.generateFunction();

        Tile tile0 = IntConstantNoDataArrayTile.fill(0, 4, 4);
        Tile tile1 = IntConstantNoDataArrayTile.fill(3, 4, 4);
        Tile tile2 = FloatConstantNoDataArrayTile.fill(1.5f, 4, 4);
        Tile tile3 = FloatConstantNoDataArrayTile.fill(3.5f, 4, 4);
        Tile nodataTile = FloatConstantNoDataArrayTile.empty(4, 4);

        Seq<Tile> result1 = transformation1.apply(JavaConversions.asScalaBuffer(Arrays.asList(nodataTile, tile0, tile1, tile2, tile3)));
        Seq<Tile> result2 = transformation2.apply(JavaConversions.asScalaBuffer(Arrays.asList(nodataTile, tile0, tile1, tile2, tile3)));

        assertTrue(result1.apply(0).isNoDataTile());
        assertEquals(1, result1.apply(1).getDouble(0,0));
        assertEquals(IntConstantNoDataArrayTile.empty(0, 0).cellType(), result1.apply(1).cellType());
        assertEquals(2, result1.apply(2).getDouble(0,0));
        assertEquals(IntConstantNoDataArrayTile.empty(0, 0).cellType(), result1.apply(2).cellType());
        assertEquals(1.5, result1.apply(3).getDouble(0,0));
        assertEquals(FloatConstantNoDataArrayTile.empty(0, 0).cellType(), result1.apply(3).cellType());
        assertEquals(2, result1.apply(4).getDouble(0,0));
        assertEquals(FloatConstantNoDataArrayTile.empty(0, 0).cellType(), result1.apply(4).cellType());

        assertTrue(result2.apply(0).isNoDataTile());
        assertEquals(1.5, result2.apply(1).getDouble(0,0));
        assertEquals(FloatConstantNoDataArrayTile.empty(0, 0).cellType(), result2.apply(1).cellType());
        assertEquals(2.5, result2.apply(2).getDouble(0,0));
        assertEquals(FloatConstantNoDataArrayTile.empty(0, 0).cellType(), result2.apply(2).cellType());
        assertEquals(1.5, result2.apply(3).getDouble(0,0));
        assertEquals(FloatConstantNoDataArrayTile.empty(0, 0).cellType(), result2.apply(3).cellType());
        assertEquals(2.5, result2.apply(4).getDouble(0,0));
        assertEquals(FloatConstantNoDataArrayTile.empty(0, 0).cellType(), result2.apply(4).cellType());
    }

    @DisplayName("Test count process")
    @Test
    public void testCount() {
        Tile tile0 = FloatConstantNoDataArrayTile.fill(0, 4, 4);
        Tile tile1 = FloatConstantNoDataArrayTile.fill(3, 4, 4);
        Tile tile2 = FloatConstantNoDataArrayTile.fill(4, 4, 4);
        Tile tile3 = FloatConstantNoDataArrayTile.fill(Float.NaN, 4, 4);

        Seq<Tile> result1 = createCount(false).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0.mutable().copy(), tile1.mutable().copy(), tile2.mutable().copy(), tile3.mutable().copy())));

        assertEquals(3, result1.apply(0).get(0, 0));

        Seq<Tile> result2 = createCount(true).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0.mutable().copy(), tile1.mutable().copy(), tile2.mutable().copy(), tile3.mutable().copy())));

        assertEquals(4, result2.apply(0).get(0, 0));

        Seq<Tile> result3 = createCount("gt", 2.0, false).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0.mutable().copy(), tile1.mutable().copy(), tile2.mutable().copy(), tile3.mutable().copy())));

        assertEquals(2, result3.apply(0).get(0, 0));

        Seq<Tile> result4 = createCount("eq", 3.0, true).generateFunction().apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0.mutable().copy(), tile1.mutable().copy(), tile2.mutable().copy(), tile3.mutable().copy())));

        assertEquals(3, result4.apply(0).get(0, 0));
    }

    private Seq<Tile> predictWithDefaultRandomForestClassifier(scala.collection.mutable.Buffer<Tile> tiles, Random random) {
        SparkConf conf = new SparkConf();
        conf.setAppName("OpenEOTest");
        conf.setMaster("local[1]");
        //conf.set("spark.driver.bindAddress", "127.0.0.1");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkContext.getOrCreate(conf);
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
        List<LabeledPoint> labels = new ArrayList<>();
        int numberOfLabels = 100;
        for (int labelIndex = 0; labelIndex < numberOfLabels; labelIndex++) {
            double[] featureValues = new double[3];
            for (int featureIndex = 0; featureIndex < featureValues.length; featureIndex++) {
                featureValues[featureIndex] = random.nextDouble() * 10;
            }
            org.apache.spark.mllib.linalg.Vector features = Vectors.dense(featureValues);
            labels.add(new LabeledPoint(random.nextInt(10), features));
        }

        JavaRDD<LabeledPoint> trainingData = sc.parallelize(labels);
        int numClasses = 10;
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        int numTrees = 3;
        String featureSubsetStrategy = "auto";
        String impurity = "gini";
        int maxDepth = 4;
        int maxBins = 32;
        int seed = 42;
        RandomForestModel model = RandomForest.trainClassifier(
            trainingData, numClasses, categoricalFeaturesInfo,
            numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed
        );
        java.util.Map<String, Object> arguments = new HashMap<>();
        arguments.put("model", new HashMap<String, String>() {{ put("from_parameter", "context"); }});
        arguments.put("data", "dummy");
        builder.expressionStart("predict_random_forest", arguments);

        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();

        builder.expressionEnd("predict_random_forest",arguments);

        Map<String, Object> javaContext = new HashMap<String, Object>() {{ put("context", model); }};
        List<Tuple2<String, Object>> contextTuples = javaContext.entrySet().stream()
                .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        scala.collection.immutable.Map<String, Object> scalaContext = scala.collection.immutable.Map$.MODULE$.apply(JavaConversions.asScalaBuffer(contextTuples).toSeq());

        Seq<Tile> result = builder
                .generateFunction(scalaContext)
                .apply(tiles.toSeq());
        SparkContext.getOrCreate().stop();
        return result;
    }


    @DisplayName("Test predict_random_forest")
    @Test
    public void testPredictRandomForest() {
        Random random = new Random(42);

        FloatArrayTile tile0 = FloatArrayTile.empty(4,4);
        FloatArrayTile tile1 = FloatArrayTile.empty(4,4);
        FloatArrayTile tile2 = FloatArrayTile.empty(4,4);
        for (int col = 0; col < 4; col++) {
            for (int row = 0; row < 4; row++) {
                tile0.setDouble(col, row, (random.nextDouble() * 10));
                tile1.setDouble(col, row, (random.nextDouble() * 10));
                tile2.setDouble(col, row, (random.nextDouble() * 10));
            }
        }
        scala.collection.mutable.Buffer<Tile> tiles = JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile1, tile2));
        Seq<Tile> result = predictWithDefaultRandomForestClassifier(tiles, random);
        assertEquals(FloatCellType.withDefaultNoData(),result.apply(0).cellType());
        assertEquals(8, result.apply(0).get(0,0));
        assertEquals(4, result.apply(0).get(0,1));
        assertEquals(4, result.apply(0).get(0,2));
        assertEquals(1, result.apply(0).get(3,2));
        assertEquals(9, result.apply(0).get(3,3));
    }

    @DisplayName("Test predict_random_forest with the wrong arguments.")
    @Test
    public void testPredictRandomForestWithWrongArguments() {
        Random random = new Random(42);

        // Not enough features.
        FloatArrayTile tile0 = FloatArrayTile.empty(4,4);
        FloatArrayTile tile1 = FloatArrayTile.empty(4,4);
        for (int col = 0; col < 4; col++) {
            for (int row = 0; row < 4; row++) {
                tile0.setDouble(col, row, (random.nextDouble() * 10));
                tile1.setDouble(col, row, (random.nextDouble() * 10));
            }
        }
        scala.collection.mutable.Buffer<Tile> tiles = JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile1));
        assertThrows(IllegalArgumentException.class, () -> predictWithDefaultRandomForestClassifier(tiles, random));

        // NoData cells.
        FloatArrayTile emptyTile0 = FloatArrayTile.empty(4,4);
        FloatArrayTile emptyTile1 = FloatArrayTile.empty(4,4);
        FloatArrayTile emptyTile2 = FloatArrayTile.empty(4,4);
        scala.collection.mutable.Buffer<Tile> emptyTiles = JavaConversions.asScalaBuffer(Arrays.asList(emptyTile0, emptyTile1, emptyTile2));
        Seq<Tile> result = predictWithDefaultRandomForestClassifier(emptyTiles, random);
        double noDataValue = emptyTile0.get(0,0);
        assertEquals(noDataValue, result.apply(0).get(0,0));
        assertEquals(noDataValue, result.apply(0).get(0,1));
        assertEquals(noDataValue, result.apply(0).get(0,2));
        assertEquals(noDataValue, result.apply(0).get(3,2));
        assertEquals(noDataValue, result.apply(0).get(3,3));
    }

    @DisplayName("Test inspect process")
    @Test
    public void testInspect() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("message","test");
        arguments.put("level", "error");

        builder.expressionStart("inspect", arguments);

        builder.constantArguments(arguments);
        builder.argumentStart("data");
        builder.argumentEnd();


        builder.expressionEnd("inspect",arguments);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile0 = ByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);
        ByteArrayTile tile1 = ByteConstantNoDataArrayTile.fill((byte) 5, 4, 4);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile0, tile1)));
        Tile res = result.apply(0);
        assertTileEquals(tile0, res);
    }

    static OpenEOProcessScriptBuilder createArrayApplyCos() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();

        Map<String, Object> argumentsCos = new HashMap<>();
        argumentsCos.put("x", Collections.singletonMap("from_parameter", "x"));
        builder.expressionStart("cos", argumentsCos);

        builder.argumentStart("x");
        builder.fromParameter("x");
        builder.argumentEnd();

        builder.expressionEnd("cos", argumentsCos);
        return builder;
    }

    static OpenEOProcessScriptBuilder createArrayApply() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();

        Map<String, Object> arguments = new HashMap<>();
        arguments.put("data", Collections.singletonMap("from_parameter", "x"));
        arguments.put("process", "dummy");
        builder.expressionStart("array_apply", arguments);

        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();

        builder.argumentStart("process");
        {
            // scope block, just for nice indentation
            Map<String, Object> argumentsCos = new HashMap<>();
            argumentsCos.put("x", Collections.singletonMap("from_parameter", "x"));
            builder.expressionStart("cos", argumentsCos);

            builder.argumentStart("x");
            builder.fromParameter("x");
            builder.argumentEnd();

            builder.expressionEnd("cos", argumentsCos);
        }
        builder.argumentEnd();

        builder.expressionEnd("array_apply", arguments);
        return builder;
    }

    static OpenEOProcessScriptBuilder createArrayApplyDateDifference(boolean fixedDate ) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();

        Map<String, Object> arguments = new HashMap<>();
        arguments.put("data", Collections.singletonMap("from_parameter", "x"));
        arguments.put("process", "dummy");
        builder.expressionStart("array_apply", arguments);

        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();

        builder.argumentStart("process");
        {
            // scope block, just for nice indentation
            Map<String, Object> argumentsCos = new HashMap<>();
            argumentsCos.put("date1", Collections.singletonMap("from_parameter", "label"));
            argumentsCos.put("unit", "day");
            if (fixedDate) {
                argumentsCos.put("date2", "2022-01-02T00:00:00Z");
            }

            builder.expressionStart("date_difference", argumentsCos);

            builder.argumentStart("date1");
            builder.fromParameter("label");
            builder.argumentEnd();
            if (!fixedDate) {
                builder.argumentStart("date2");
                Map<String, Object> argumentsReplace = new HashMap<>();
                argumentsReplace.put("value",15);
                argumentsReplace.put("component", "day");
                argumentsReplace.put("date", Collections.singletonMap("from_parameter", "label"));
                builder.expressionStart("date_replace_component", argumentsReplace);
                builder.argumentStart("date");
                builder.fromParameter("label");
                builder.argumentEnd();
                builder.expressionEnd("date_replace_component",argumentsReplace);
                //string constant is not yet transmitted
                builder.argumentEnd();
            }



            builder.expressionEnd("date_difference", argumentsCos);
        }
        builder.argumentEnd();

        builder.expressionEnd("array_apply", arguments);
        return builder;
    }

    static OpenEOProcessScriptBuilder createRankComposite() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();

        Map<String, Object> arguments = new HashMap<>();
        arguments.put("data", Collections.singletonMap("from_parameter", "x"));
        arguments.put("process", "dummy");
        builder.expressionStart("array_apply", arguments);

        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();

        builder.argumentStart("process");
        {
            // scope block, just for nice indentation
            Map<String, Object> argumentsCos = new HashMap<>();
            argumentsCos.put("x", Collections.singletonMap("from_parameter", "x"));
            argumentsCos.put("y", "dummy");
            builder.expressionStart("eq", argumentsCos);

            builder.argumentStart("x");
            builder.fromParameter("x");
            builder.argumentEnd();
            builder.argumentStart("y");
            Map<String, Object> argsMax = Collections.singletonMap("data", Collections.singletonMap("from_parameter", "data"));
            builder.expressionStart("max", argsMax);
            builder.argumentStart("data");
            builder.fromParameter("data");//some overloading of "data" argument going on here
            builder.argumentEnd();
            builder.expressionEnd("max",argsMax);
            builder.argumentEnd();

            builder.expressionEnd("eq", argumentsCos);
        }
        builder.argumentEnd();

        builder.expressionEnd("array_apply", arguments);
        return builder;
    }

    @DisplayName("Test array_apply process")
    @Test
    public void testArrayApply() {
        for (OpenEOProcessScriptBuilder builder : Arrays.asList(createArrayApplyCos(), createArrayApply())) {
            Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
            Tile tile0 = FloatConstantNoDataArrayTile.fill(1, 4, 4);
            Tile tile1 = FloatConstantNoDataArrayTile.fill(3, 4, 4);
            Tile tile2 = FloatConstantNoDataArrayTile.fill(-1, 4, 4);
            Tile tile3 = FloatConstantNoDataArrayTile.fill(1.9f, 4, 4);
            Tile nodataTile = ByteConstantNoDataArrayTile.empty(4, 4);

            Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(nodataTile, tile0, tile1, tile2, tile3)));

            assertTrue(result.apply(0).isNoDataTile());

            assertEquals(Math.cos(tile0.getDouble(0, 0)), result.apply(1).getDouble(0, 0), 0.001);
            assertEquals(Math.cos(tile1.getDouble(0, 0)), result.apply(2).getDouble(0, 0), 0.001);
            assertEquals(Math.cos(tile2.getDouble(0, 0)), result.apply(3).getDouble(0, 0), 0.001);
            assertEquals(Math.cos(tile3.getDouble(0, 0)), result.apply(4).getDouble(0, 0), 0.001);
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = {false,true})
    @DisplayName("Test array_apply with date difference process")
    public void testArrayApplyDateDifference(boolean fixedDate) {
        OpenEOProcessScriptBuilder builder = createArrayApplyDateDifference(fixedDate);
        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction(Collections.singletonMap("array_labels",JavaConversions.asScalaBuffer(Arrays.asList("2022-01-04T04:00:00Z","2022-01-05T00:00:00Z","2016-02-29T00:00:00Z","2019-06-15T00:00:00Z","2030-12-31T00:00:00Z"))));
        Tile tile0 = FloatConstantNoDataArrayTile.fill(1, 4, 4);
        Tile tile1 = FloatConstantNoDataArrayTile.fill(3, 4, 4);
        Tile tile2 = FloatConstantNoDataArrayTile.fill(-1, 4, 4);
        Tile tile3 = FloatConstantNoDataArrayTile.fill(1.9f, 4, 4);
        Tile nodataTile = ByteConstantNoDataArrayTile.empty(4, 4);

        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(nodataTile, tile0, tile1, tile2, tile3)));

        if (fixedDate) {
            assertEquals(-2.16666, result.apply(0).getDouble(0, 0),0.001);
            assertEquals(-3, result.apply(1).getDouble(0, 0));
            assertEquals(2134, result.apply(2).getDouble(0, 0));
            assertEquals(932, result.apply(3).getDouble(0, 0));
        }else{
            assertEquals(11, result.apply(0).getDouble(0, 0));
            assertEquals(10, result.apply(1).getDouble(0, 0));
            assertEquals(-14, result.apply(2).getDouble(0, 0));
            assertEquals(0, result.apply(3).getDouble(0, 0));
        }

    }

    @Test
    public void testBAP() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();

        builder.expressionStart("array_apply", Collections.EMPTY_MAP);
        builder.argumentStart("data");
            builder.expressionStart("array_apply", Collections.EMPTY_MAP);
            builder.argumentStart("data");
            builder.fromParameter("data");
            builder.argumentEnd();
            builder.argumentStart("process");
            builder.expressionStart("add", Collections.EMPTY_MAP);
            builder.argumentStart("x");
            builder.expressionStart("absolute", Collections.EMPTY_MAP);
            builder.argumentStart("x");

            builder.expressionStart("multiply", Collections.EMPTY_MAP);
            builder.constantArgument("x", 15);
            builder.argumentStart("y");
            builder.fromParameter("x");
            builder.argumentEnd();

            builder.expressionEnd("multiply", map2("x",15,"y",Collections.singletonMap("from_parameter", "x")));
            builder.argumentEnd();
            Map<String, Object> absArgs = map1("x", "dummy");
            builder.expressionEnd("absolute", absArgs);
            builder.argumentEnd();
            builder.argumentStart("y");
            builder.expressionStart("absolute", absArgs);
            builder.argumentStart("x");
            builder.expressionStart("date_difference", Collections.EMPTY_MAP);
            builder.argumentStart("date1");
            builder.expressionStart("date_replace_component", Collections.EMPTY_MAP);
            builder.argumentStart("date");
            builder.fromParameter("label");
            builder.argumentEnd();
            builder.constantArgument("value", 15);
            Map<String, Object> dateReplaceArgs = map3("component", "day", "date", fromParam("label"), "value", 15);
            builder.expressionEnd("date_replace_component", dateReplaceArgs);
            builder.argumentEnd();
            builder.argumentStart("date2");
            builder.fromParameter("label");
            builder.argumentEnd();
            builder.expressionEnd("date_difference", map3("date1", Collections.EMPTY_MAP, "date2", fromParam("label"), "unit", "day"));
            builder.argumentEnd();
            builder.expressionEnd("absolute", absArgs);
            builder.argumentEnd();
            builder.expressionEnd("add", map2("x", Collections.EMPTY_MAP, "y", Collections.EMPTY_MAP));
            builder.argumentEnd();
            builder.expressionEnd("array_apply", Collections.EMPTY_MAP);
        builder.argumentEnd();
        builder.argumentStart("process");
        builder.expressionStart("neq",Collections.EMPTY_MAP);
            builder.argumentStart("x");
                builder.expressionStart("int", Collections.EMPTY_MAP);
                builder.argumentStart("x");
                builder.fromParameter("x");
                builder.argumentEnd();
                builder.expressionEnd("int", map1("x", fromParam( "x")));
            builder.argumentEnd();
            builder.argumentStart("y");
                builder.expressionStart("int", Collections.EMPTY_MAP);
                builder.argumentStart("x");
                builder.expressionStart("min", Collections.EMPTY_MAP);
                builder.argumentStart("data");
                    builder.expressionStart("array_apply", Collections.EMPTY_MAP);
                    builder.argumentStart("data");
                    //this second "data" parameter now resolves to "data" of the first array apply instead of the original "data"
                    builder.fromParameter("parent.data");
                    builder.argumentEnd();
                    builder.argumentStart("process");
                    builder.expressionStart("add", Collections.EMPTY_MAP);
                    builder.argumentStart("x");
                    builder.expressionStart("absolute", absArgs);
                    builder.argumentStart("x");
                    builder.expressionStart("multiply", Collections.EMPTY_MAP);
                    builder.constantArgument("x", 15);
                    builder.argumentStart("y");
                    builder.fromParameter("x");
                    builder.argumentEnd();
                    builder.expressionEnd("multiply",map2("x", 15, "y", fromParam("x")));
                    builder.argumentEnd();
                    builder.expressionEnd("absolute", absArgs);
                    builder.argumentEnd();
                    builder.argumentStart("y");
                    builder.expressionStart("absolute", absArgs);
                    builder.argumentStart("x");
                    builder.expressionStart("date_difference", Collections.EMPTY_MAP);
                    builder.argumentStart("date1");
                    builder.expressionStart("date_replace_component", Collections.EMPTY_MAP);
                    builder.argumentStart("date");
                    builder.fromParameter("label");
                    builder.argumentEnd();
                    builder.constantArgument("value", 15);
                    builder.expressionEnd("date_replace_component", dateReplaceArgs);
                    builder.argumentEnd();
                    builder.argumentStart("date2");
                    builder.fromParameter("label");
                    builder.argumentEnd();
                    builder.expressionEnd("date_difference", map3("date1", Collections.EMPTY_MAP, "date2", fromParam("label"), "unit", "day"));
                    builder.argumentEnd();
                    builder.expressionEnd("absolute", absArgs);
                    builder.argumentEnd();
                    builder.expressionEnd("add", map2("x","dummy","y","dummy"));
                    builder.argumentEnd();
                    builder.expressionEnd("array_apply",Collections.EMPTY_MAP);
                builder.argumentEnd();
                builder.expressionEnd("min",map1("data","dummy"));
                builder.argumentEnd();
                builder.expressionEnd("int", Collections.EMPTY_MAP);
            builder.argumentEnd();
        builder.expressionEnd("neq", map2("x","dummy","y","dummy"));
        builder.argumentEnd();
        builder.expressionEnd("array_apply", Collections.EMPTY_MAP);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction(Collections.singletonMap("array_labels",JavaConversions.asScalaBuffer(Arrays.asList("2022-01-04T04:00:00Z","2022-01-05T00:00:00Z","2022-01-14T00:00:00Z","2022-01-16T00:00:00Z","2022-01-21T00:00:00Z"))));
        Tile tile0 = FloatConstantNoDataArrayTile.fill(5, 1, 1);
        Tile tile1 = FloatConstantNoDataArrayTile.fill(3, 1, 1);
        Tile tile2 = FloatConstantNoDataArrayTile.fill(1, 1, 1);
        Tile tile3 = FloatConstantNoDataArrayTile.fill(1.9f, 1, 1);
        Tile nodataTile = ByteConstantNoDataArrayTile.empty(1, 1);

        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(nodataTile, tile0, tile1, tile2, tile3)));
        BitConstantTile trueTile = new BitConstantTile(true, 1, 1);
        assertTileEquals(trueTile,result.apply(0));
        assertTileEquals(trueTile,result.apply(1));
        assertTileEquals(trueTile,result.apply(2));
        assertTileEquals(new BitConstantTile(false,1,1),result.apply(3));
        assertTileEquals(trueTile,result.apply(4));

    }

    @Test
    public void testDateBetweenIntervalAfter() {
        testDateBetween("2023-01-02T00:00:00Z","2023-01-15T00:00:00Z",false,true);
    }

    @Test
    public void testDateBetweenIntervalBefore() {
        testDateBetween("2022-01-01T00:00:00Z","2022-01-01T12:00:00Z",false,true);
    }

    @Test
    public void testDateBetweenActuallyBetween() {
        testDateBetween("2022-01-02T00:00:00Z","2022-01-15T00:00:00Z",true,true);
    }

    @Test
    public void testDateBetweenEndExclusive() {
        testDateBetween("2022-01-01T00:00:00Z","2022-01-02T00:00:00Z",false,true);
    }

    @Test
    public void testDateBetweenEndInclusive() {
        testDateBetween("2022-01-01T00:00:00Z","2022-01-02T00:00:00Z",true,false);
    }

    void testDateBetween(Object min, Object max, Boolean expected, Object excludeMax) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> args = map3("min", min, "max", max, "exclude_max",excludeMax);
        builder.expressionStart("date_between", args);
        builder.argumentStart("x");
        builder.fromParameter("value");
        builder.argumentEnd();
        //builder.constantArgument("min", (byte) 9);
        //builder.constantArgument("max", (byte) 11);
        builder.expressionEnd("date_between", args);

        Object transformation = builder.generateAnyFunction(Collections.singletonMap("value","2022-01-02T00:00:00Z")).apply("2022-01-02T00:00:00Z");
        assertEquals(transformation,expected);

    }

    static OpenEOProcessScriptBuilder createMedian(Boolean ignoreNoData) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = ignoreNoData!=null? Collections.singletonMap("ignore_nodata",ignoreNoData.booleanValue()) : Collections.emptyMap();
        builder.expressionStart("median", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();

        if (ignoreNoData != null) {
            builder.constantArgument("ignore_nodata",ignoreNoData.booleanValue());
        }

        builder.expressionEnd("median",arguments);
        return builder;
    }

    static OpenEOProcessScriptBuilder createStandardDeviation(Boolean ignoreNoData) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = new HashMap<>();
        if (ignoreNoData != null) arguments.put("ignore_nodata", ignoreNoData.booleanValue());
        arguments.put("data", "dummy");
        builder.expressionStart("sd", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();

        if (ignoreNoData != null) {
            builder.constantArgument("ignore_nodata",ignoreNoData.booleanValue());
        }

        builder.expressionEnd("sd",arguments);
        return builder;
    }


    static OpenEOProcessScriptBuilder createQuantiles(Boolean ignoreNoData, int qValue) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = ignoreNoData!=null? map2("ignore_nodata",ignoreNoData.booleanValue(),"q",qValue) : map1("q",qValue);
        builder.expressionStart("quantiles", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();
        builder.constantArgument("q",2);

        if (ignoreNoData != null) {
            builder.constantArgument("ignore_nodata",ignoreNoData.booleanValue());
        }


        builder.expressionEnd("quantiles",arguments);
        return builder;
    }

    static OpenEOProcessScriptBuilder createFeatureEngineering() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        builder.expressionStart("array_concat",Collections.emptyMap());
        builder.argumentStart("array1");
        List<Double> percentiles = Arrays.asList(0.25, 0.5, 0.75);
        builder.expressionStart("quantiles", map1("probabilities", percentiles));
        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();
        builder.arrayStart("probabilities");
        builder.constantArrayElement(0.25);
        builder.constantArrayElement(0.5);
        builder.constantArrayElement(0.75);
        builder.arrayEnd();
        builder.expressionEnd("quantiles", map1( "probabilities", percentiles));
        builder.argumentEnd();
        builder.arrayStart("array2");
        builder.expressionStart("sd", map1("data","dummy"));
        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();
        builder.expressionEnd("sd", map1("data","dummy"));
        builder.arrayElementDone();
        builder.expressionStart("mean", Collections.emptyMap());
        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();
        builder.expressionEnd("mean",map1("data","dummy"));
        builder.arrayElementDone();
        builder.arrayEnd();
        builder.expressionEnd("array_concat", map1("data","dummy"));
        return builder;

    }

    static OpenEOProcessScriptBuilder createMonthSelection() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        builder.expressionStart("array_concat",Collections.emptyMap());
        builder.arrayStart("array1");

        builder.expressionStart("array_element", map1("index", 3));
        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();
        builder.expressionEnd("array_element", map1( "index", 3));

        builder.arrayElementDone();
        builder.arrayEnd();
        builder.arrayStart("array2");

        builder.expressionStart("array_element", map1("index", 8));
        builder.argumentStart("data");
        builder.fromParameter("data");
        builder.argumentEnd();
        builder.expressionEnd("array_element", map1( "index", 8));

        builder.arrayElementDone();
        builder.arrayEnd();
        builder.expressionEnd("array_concat", map1("data","dummy"));
        return builder;

    }


    static OpenEOProcessScriptBuilder createArrayInterpolateLinear() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = Collections.emptyMap();
        builder.expressionStart("array_interpolate_linear", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();

        builder.expressionEnd("array_interpolate_linear",arguments);
        return builder;
    }

    static OpenEOProcessScriptBuilder createLinearScaleRange(Number inputMin, Number inputMax,Number outputMin, Number outputMax) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("inputMin", inputMin);
        arguments.put("inputMax", inputMax);
        if (outputMin != null) {
            arguments.put("outputMin", outputMin);
        }
        if (outputMax != null) {
            arguments.put("outputMax", outputMax);
        }
        builder.expressionStart("linear_scale_range", arguments);

        builder.argumentStart("x");
        builder.argumentEnd();
        builder.constantArguments(arguments);



        builder.expressionEnd("linear_scale_range",arguments);
        return builder;
    }

    static OpenEOProcessScriptBuilder createClip(Number min, Number max) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("min", min);
        arguments.put("max", max);

        builder.expressionStart("clip", arguments);

        builder.argumentStart("x");
        builder.argumentEnd();
        builder.constantArguments(arguments);

        builder.expressionEnd("clip", arguments);
        return builder;
    }

    static OpenEOProcessScriptBuilder createCount(boolean countAll) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> arguments = new HashMap<>();
        if (countAll)
            arguments.put("condition", true);
        else
            arguments.put("condition", null);

        builder.expressionStart("count", arguments);

        builder.argumentStart("data");
        builder.argumentEnd();
        builder.constantArguments(arguments);

        builder.expressionEnd("count", arguments);

        return builder;
    }

    static OpenEOProcessScriptBuilder createCount(String xyOperator, Double yValue, boolean negation) {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();

        builder.expressionStart("count", dummyMap("data", "condition"));
        builder.argumentStart("data");
        builder.argumentEnd();
        builder.argumentStart("condition");
        if (negation) {
            builder.expressionStart("not", dummyMap("x"));
            builder.argumentStart("x");
        }
        builder.expressionStart(xyOperator, dummyMap("x", "y"));
        builder.argumentStart("x");
        builder.argumentEnd();
        builder.constantArgument("y", yValue);
        builder.expressionEnd(xyOperator, dummyMap("x", "y"));
        if (negation) {
            builder.argumentEnd();
            builder.expressionEnd("not", dummyMap("x"));
        }
        builder.argumentEnd();
        builder.expressionEnd("count", dummyMap("data", "condition"));

        return builder;
    }

    private static void buildArrayElementProcess(OpenEOProcessScriptBuilder builder, Integer index) {
        Map<String, Object> args = Collections.singletonMap("index", index);
        builder.expressionStart("array_element", args);
        builder.constantArgument("index", index);
        builder.argumentStart("data");
        builder.argumentEnd();
        builder.expressionEnd("array_element", args);
    }

    private static void buildBandArray(OpenEOProcessScriptBuilder builder, String argName) {
        builder.arrayStart(argName);
        buildArrayElementProcess(builder, 0);
        builder.arrayElementDone();
        buildArrayElementProcess(builder, 1);
        builder.arrayElementDone();
        builder.arrayEnd();
    }

    private static void buildBandXYArguments(OpenEOProcessScriptBuilder builder, Integer xIndex, Integer yIndex) {
        builder.argumentStart("x");
        buildArrayElementProcess(builder, xIndex);
        builder.argumentEnd();
        builder.argumentStart("y");
        buildArrayElementProcess(builder, yIndex);
        builder.argumentEnd();
    }

    private static BitArrayTile fillBitArrayTile(int cols, int rows, int... values) {
        BitArrayTile tile = BitArrayTile.ofDim(cols, rows);
        for (int i = 0; i < Math.min(cols * rows, values.length); i++) {
            tile.set(i % cols, i / cols, values[i] == 0 ? 0 : 1);
        }
        return tile;
    }

    private static ByteArrayTile fillByteArrayTile(int cols, int rows, int... values) {
        ByteArrayTile tile = ByteArrayTile.ofDim(cols, rows);
        for (int i = 0; i < Math.min(cols * rows, values.length); i++) {
            tile.set(i % cols, i / cols, values[i]);
        }
        return tile;
    }

    private static IntArrayTile fillIntArrayTile(int cols, int rows, int... values) {
        IntArrayTile tile = IntArrayTile.ofDim(cols, rows);
        for (int i = 0; i < Math.min(cols * rows, values.length); i++) {
            tile.set(i % cols, i / cols, values[i]);
        }
        return tile;
    }

    private static DoubleArrayTile fillDoubleArrayTile(int cols, int rows, double... values) {
        DoubleArrayTile tile = DoubleArrayTile.ofDim(cols, rows);
        for (int i = 0; i < Math.min(cols * rows, values.length); i++) {
            tile.setDouble(i % cols, i / cols, values[i]);
        }
        return tile;
    }

    private static FloatArrayTile fillFloatArrayTile(int cols, int rows, double... values) {
        FloatArrayTile tile = FloatArrayTile.ofDim(cols, rows);
        for (int i = 0; i < Math.min(cols * rows, values.length); i++) {
            tile.setDouble(i % cols, i / cols, values[i]);
        }
        return tile;
    }

    private static Map<String, Object> dummyMap(String... keys) {
        Map<String, Object> m = new HashMap<String, Object>();
        for (String key : keys) {
            m.put(key, "dummy");
        }
        return m;
    }

    /**
     * Build 1-item Map<String, Object>
     */
    private static Map<String, Object> map1(String k, Object v) {
        Map<String, Object> m = new HashMap<String, Object>(1);
        m.put(k, v);
        return m;
    }

    private static Map<String, Object> fromParam(String p) {
        return Collections.singletonMap("from_parameter",p);
    }

    /**
     * Build 2-item Map<String, Object>
     */
    private static Map<String, Object> map2(String k1, Object v1, String k2, Object v2) {
        Map<String, Object> m = new HashMap<String, Object>(2);
        m.put(k1, v1);
        m.put(k2, v2);
        return m;
    }

    /**
     * Build 3-item Map<String, Object>
     */
    private static Map<String, Object> map3(String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        Map<String, Object> m = new HashMap<String, Object>(3);
        m.put(k1, v1);
        m.put(k2, v2);
        m.put(k3, v3);
        return m;
    }

    /**
     * Build 4-item Map<String, Object>
     */
    private static Map<String, Object> map4(String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4) {
        Map<String, Object> m = new HashMap<String, Object>(3);
        m.put(k1, v1);
        m.put(k2, v2);
        m.put(k3, v3);
        m.put(k4, v4);
        return m;
    }

    private void assertTileEquals(Tile expected, Tile actual) {
        System.out.println("Expected: " + expected.cols() + "x" + expected.rows() + " " + expected.cellType() + " " + Arrays.toString(expected.toArray()));
        System.out.println("Actual:   " + actual.cols() + "x" + actual.rows() + " " + actual.cellType() + " " + Arrays.toString(actual.toArray()));
        assertEquals(expected.cols(), actual.cols());
        assertEquals(expected.rows(), actual.rows());
        assertEquals(expected.cellType(), actual.cellType());
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    private void assertDoubleTileEquals(Tile expected, Tile actual) {
        System.out.println("Expected: " + expected.cols() + "x" + expected.rows() + " " + expected.cellType() + " " + Arrays.toString(expected.toArrayDouble()));
        System.out.println("Actual:   " + actual.cols() + "x" + actual.rows() + " " + actual.cellType() + " " + Arrays.toString(actual.toArrayDouble()));
        assertEquals(expected.cols(), actual.cols());
        assertEquals(expected.rows(), actual.rows());
        assertEquals(expected.cellType(), actual.cellType());
        assertArrayEquals(expected.toArrayDouble(), actual.toArrayDouble());
    }
}
