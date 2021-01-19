package org.openeo.geotrellis;


import geotrellis.raster.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Function1;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

    private static Map<String, Object> dummyMap(String... keys) {
        Map<String, Object> m = new HashMap<String, Object>();
        for (String key : keys) {
            m.put(key, "dummy");
        }
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
