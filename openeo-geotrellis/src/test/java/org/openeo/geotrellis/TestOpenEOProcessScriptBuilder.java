package org.openeo.geotrellis;


import geotrellis.raster.ByteArrayTile;
import geotrellis.raster.ByteConstantNoDataArrayTile;
import geotrellis.raster.Tile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Function1;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
