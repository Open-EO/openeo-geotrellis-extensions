package org.openeo.geotrellis;


import geotrellis.raster.ByteArrayTile;
import geotrellis.raster.ByteConstantNoDataArrayTile;
import geotrellis.raster.Tile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Function1;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

import java.util.Arrays;
import java.util.Collections;

public class TestOpenEOProcessScriptBuilder {

    @DisplayName("Test NDVI process graph")
    @Test
    public void testNDVIScript() {
        OpenEOProcessScriptBuilder builder = new OpenEOProcessScriptBuilder();
        Buffer<String> empty = JavaConversions.asScalaBuffer(Collections.emptyList());
        builder.expressionStart("divide", empty);
        builder.argumentStart("x");
            builder.expressionStart("sum", empty);
                builder.argumentStart("data");
                builder.argumentEnd();
            builder.expressionEnd("sum",empty);
        builder.argumentEnd();

        builder.argumentStart("y");
        builder.expressionStart("subtract", empty);
        builder.argumentStart("data");
        builder.argumentEnd();
        builder.expressionEnd("subtract",empty);
        builder.argumentEnd();

        builder.expressionEnd("divide", empty);
        Function1<Seq<Tile>, Seq<Tile>> transformation = builder.generateFunction();
        ByteArrayTile tile1 = ByteConstantNoDataArrayTile.fill((byte) 10, 4, 4);
        ByteArrayTile tile2 = ByteConstantNoDataArrayTile.fill((byte) 5, 4, 4);
        Seq<Tile> result = transformation.apply(JavaConversions.asScalaBuffer(Arrays.asList(tile1, tile2)));
        Tile ndvi = result.apply(0);

        System.out.println("Arrays.toString(ndvi.toArrayDouble()) = " + Arrays.toString(ndvi.toArrayDouble()));
    }
}
