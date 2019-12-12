package org.openeo.geotrellis;


import geotrellis.raster.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Function1;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestOpenEOProcessScriptBuilder {

    @DisplayName("Test NDVI process graph")
    @Test
    public void testNDVIScript() {
        OpenEOProcessScriptBuilder builder = createNormalizedDifferenceProcess();
        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile1 = ByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);
        ByteArrayTile tile2 = ByteConstantNoDataArrayTile.fill((byte) 5, 4, 4);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1, tile2)));
        Tile ndvi = result.apply(0);

        System.out.println("Arrays.toString(ndvi.toArrayDouble()) = " + Arrays.toString(ndvi.toArrayDouble()));
        assertEquals(3.0, ndvi.get(0, 0));
    }

    @DisplayName("Test NDVI process graph with band selection")
    @Test
    public void testNDVIScriptBandSelection() {
        OpenEOProcessScriptBuilder builder = createNormalizedDifferenceProcess2();
        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile1 = ByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);
        ByteArrayTile tile2 = ByteConstantNoDataArrayTile.fill((byte) 5, 4, 4);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1, tile2)));
        Tile ndvi = result.apply(0);

        System.out.println("Arrays.toString(ndvi.toArrayDouble()) = " + Arrays.toString(ndvi.toArrayDouble()));
        assertEquals(3.0, ndvi.get(0, 0));
    }

    static OpenEOProcessScriptBuilder createNormalizedDifferenceProcess() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> empty = Collections.emptyMap();
        builder.expressionStart("divide", empty);

        builder.arrayStart("data");

        builder.expressionStart("sum", empty);
        builder.argumentStart("data");
        builder.argumentEnd();
        builder.expressionEnd("sum",empty);
        builder.arrayElementDone();

        builder.expressionStart("subtract", empty);
        builder.argumentStart("data");
        builder.argumentEnd();
        builder.expressionEnd("subtract",empty);
        builder.arrayElementDone();

        builder.arrayEnd();

        builder.expressionEnd("divide", empty);
        return builder;
    }

    /**
     * This normalized difference process actually selects bands from the array using array_element
     * @return
     */
    static OpenEOProcessScriptBuilder createNormalizedDifferenceProcess2() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> empty = Collections.emptyMap();
        builder.expressionStart("divide", empty);

        builder.arrayStart("data");

        builder.expressionStart("sum", empty);
        specifyBands(builder);
        builder.expressionEnd("sum",empty);
        builder.arrayElementDone();

        builder.expressionStart("subtract", empty);
        specifyBands(builder);
        builder.expressionEnd("subtract",empty);
        builder.arrayElementDone();

        builder.arrayEnd();

        builder.expressionEnd("divide", empty);
        return builder;
    }

    private static void specifyBands(OpenEOProcessScriptBuilder builder) {
        builder.arrayStart("data");

        Map<String, Object> args = Collections.singletonMap("index", 0);
        builder.expressionStart("array_element", args);
        builder.constantArgument("index",0);
        builder.argumentStart("data");
        builder.argumentEnd();
        builder.expressionEnd("array_element",args);
        builder.arrayElementDone();

        args = Collections.singletonMap("index", 1);
        builder.expressionStart("array_element",args);
        builder.constantArgument("index",1);
        builder.argumentStart("data");
        builder.argumentEnd();
        builder.expressionEnd("array_element",args);
        builder.arrayElementDone();

        builder.arrayEnd();
    }

    @DisplayName("Test add constant")
    @Test
    public void testAddConstant() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> empty = Collections.emptyMap();
        builder.expressionStart("sum", empty);

        builder.arrayStart("data");
        builder.constantArrayElement(10);
        builder.constantArrayElement(20);
        builder.arrayEnd();

        builder.expressionEnd("sum",empty);


        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile1 = ByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);
        ByteArrayTile tile2 = ByteConstantNoDataArrayTile.fill((byte) 5, 4, 4);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1, tile2)));
        Tile ndvi = result.apply(0);

        System.out.println("Arrays.toString(ndvi.toArrayDouble()) = " + Arrays.toString(ndvi.toArrayDouble()));
        assertEquals(30.0, ndvi.get(0, 0));
    }

    @DisplayName("Test logical Or")
    @Test
    public void testLogicalOr() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> empty = Collections.emptyMap();
        builder.expressionStart("or", empty);
        //not specifying expressions means that we're working on the input coming from the tiles?
        //see https://github.com/Open-EO/openeo-processes/issues/87#issuecomment-559448790
        builder.expressionEnd("or",empty);

        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        Tile tile1 = BitArrayTile.fill(true, 4, 4);
        Tile tile2 = BitArrayTile.fill(false, 4, 4);

        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1,tile2)));
        Tile merged = result.apply(0);

        System.out.println("Arrays.toString(ndvi.toArrayDouble()) = " + Arrays.toString(merged.toBytes()));
        assertEquals(1, merged.get(0, 0));
    }

    @DisplayName("Test logical operations")
    @Test
    public void testLogicalEquals() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> empty = Collections.emptyMap();
        builder.expressionStart("eq", empty);
        builder.argumentStart("x");
        builder.argumentEnd();
        builder.constantArgument("y",(byte)10);
        builder.expressionEnd("eq",empty);


        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        Tile tile1 = UByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);

        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1)));
        Tile ndvi = result.apply(0);

        System.out.println("Arrays.toString(ndvi.toArrayDouble()) = " + Arrays.toString(ndvi.toBytes()));
        assertEquals(1, ndvi.get(0, 0));
    }

    @DisplayName("Test logical operations: not after equals")
    @Test
    public void testLogicalNot() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> empty = Collections.emptyMap();
        builder.expressionStart("not", empty);
        builder.argumentStart("expression");
        builder.expressionStart("eq", empty);
        builder.argumentStart("x");
        builder.argumentEnd();
        builder.constantArgument("y",(byte)10);
        builder.expressionEnd("eq",empty);
        builder.argumentEnd();
        builder.expressionEnd("not",empty);


        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        Tile tile1 = UByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);

        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1)));
        Tile ndvi = result.apply(0);

        System.out.println("Arrays.toString(ndvi.toArrayDouble()) = " + Arrays.toString(ndvi.toBytes()));
        assertEquals(0, ndvi.get(0, 0));
    }

    @DisplayName("Test proper error handling in case argument is missing.")
    @Test
    public void testException() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> empty = Collections.emptyMap();
        builder.expressionStart("not", empty);
        builder.argumentStart("expression");
        builder.expressionStart("eq", empty);
        //builder.argumentStart("x");
        //builder.argumentEnd();

        builder.constantArgument("y",(byte)10);

        assertThrows(IllegalArgumentException.class,() -> {
            builder.expressionEnd("eq", empty);
        });

    }

    @DisplayName("Test logical equals, not equal constant")
    @Test
    public void testLogicalEquals2() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Map<String, Object> empty = Collections.emptyMap();
        builder.expressionStart("eq", empty);
        builder.argumentStart("x");
        builder.argumentEnd();
        builder.constantArgument("y",(byte)11);
        builder.expressionEnd("eq",empty);


        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        Tile tile1 = UByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);

        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1)));
        Tile ndvi = result.apply(0);

        System.out.println("Arrays.toString(ndvi.toArrayDouble()) = " + Arrays.toString(ndvi.toBytes()));
        assertEquals(0, ndvi.get(0, 0));
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
        ByteArrayTile tile1 = ByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);
        ByteArrayTile tile2 = ByteConstantNoDataArrayTile.fill((byte) 5, 4, 4);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1, tile2)));
        Tile ndvi = result.apply(0);

        System.out.println("Arrays.toString(ndvi.toArrayDouble()) = " + Arrays.toString(ndvi.toArrayDouble()));
        assertEquals(5, ndvi.get(0, 0));
    }
}
