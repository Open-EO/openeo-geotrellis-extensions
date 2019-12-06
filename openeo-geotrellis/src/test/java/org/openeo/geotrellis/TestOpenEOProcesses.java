package org.openeo.geotrellis;


import geotrellis.layer.*;
import geotrellis.raster.*;
import geotrellis.spark.ContextRDD;
import geotrellis.spark.testkit.TileLayerRDDBuilders$;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaPairRDD$;
import org.apache.spark.rdd.RDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;


public class TestOpenEOProcesses {

    @BeforeClass
    public static void sparkContext() {

        SparkConf conf = new SparkConf();
        conf.setAppName("OpenEOTest");
        conf.setMaster("local[4]");
        conf.set("spark.driver.bindAddress", "127.0.0.1");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkContext.getOrCreate(conf);


    }

    @AfterClass
    public static void shutDownSparkContext() {
        SparkContext.getOrCreate().stop();
    }

    @Test
    public void testSimpleUnaryProcess() {
        Tile zeroTile = new ByteConstantTile((byte)0,256,256, (ByteCells) CellType$.MODULE$.fromName("int8raw"));
        RDD<Tuple2<SpatialKey, MultibandTile>> datacube = TileLayerRDDBuilders$.MODULE$.createMultibandTileLayerRDD(SparkContext.getOrCreate(), new ArrayMultibandTile(new Tile[]{zeroTile}), new TileLayout(1, 1, 256, 256));
        RDD<Tuple2<SpatialKey, MultibandTile>> cosDatacube = new OpenEOProcesses().applyProcess(datacube, "cos");
        List<Tuple2<SpatialKey, MultibandTile>> result = cosDatacube.toJavaRDD().collect();
        System.out.println("result = " + result);
        double[] doubles = result.get(0)._2().band(0).toArrayDouble();
        assertEquals(1, result.get(0)._2().bandCount());

        assertEquals(1.0,doubles[0],0.0);
    }

    @DisplayName("Test combining all bands.")
    @Test
    public void testMapBands() {
        OpenEOProcessScriptBuilder processBuilder = TestOpenEOProcessScriptBuilder.createNormalizedDifferenceProcess();
        Tile tile10 = new ByteConstantTile((byte)10,256,256, (ByteCells) CellType$.MODULE$.fromName("int8raw"));
        Tile tile5 = new ByteConstantTile((byte)5,256,256, (ByteCells) CellType$.MODULE$.fromName("int8raw"));
        RDD<Tuple2<SpatialKey, MultibandTile>> datacube = TileLayerRDDBuilders$.MODULE$.createMultibandTileLayerRDD(SparkContext.getOrCreate(), new ArrayMultibandTile(new Tile[]{tile10,tile5}), new TileLayout(1, 1, 256, 256));
        ClassTag<SpatialKey> tag = scala.reflect.ClassTag$.MODULE$.apply(SpatialKey.class);
        RDD<Tuple2<SpatialKey, MultibandTile>> ndviDatacube = new OpenEOProcesses().<SpatialKey>mapBandsGeneric(datacube, processBuilder,tag);
        List<Tuple2<SpatialKey, MultibandTile>> result = ndviDatacube.toJavaRDD().collect();
        System.out.println("result = " + result);
        double[] doubles = result.get(0)._2().band(0).toArrayDouble();
        assertEquals(1, result.get(0)._2().bandCount());

        assertEquals(3.0,doubles[0],0.0);
    }

    @Test
    public void testMapToIntervals() {
        Tile zeroTile = new ByteConstantTile((byte)0,256,256, (ByteCells) CellType$.MODULE$.fromName("int8raw"));
        ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = (ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>>) TileLayerRDDBuilders$.MODULE$.createMultibandTileLayerRDD(SparkContext.getOrCreate(), new ArrayMultibandTile(new Tile[]{zeroTile}), new TileLayout(1, 1, 256, 256));
        JavaPairRDD<SpaceTimeKey, MultibandTile> spacetimeDataCube = JavaPairRDD$.MODULE$.fromJavaRDD(datacube.toJavaRDD()).flatMapToPair(spatialKeyMultibandTileTuple2 -> Arrays.asList(
                Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(ZonedDateTime.parse("2017-01-01T00:00:00Z"))), spatialKeyMultibandTileTuple2._2),
                Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(ZonedDateTime.parse("2017-01-15T00:00:00Z"))), spatialKeyMultibandTileTuple2._2),
                Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(ZonedDateTime.parse("2017-02-01T00:00:00Z"))), spatialKeyMultibandTileTuple2._2),
                Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(ZonedDateTime.parse("2018-01-15T00:00:00Z"))), spatialKeyMultibandTileTuple2._2)
        ).iterator());
        //JavaConverters.
        RDD<Tuple2<SpaceTimeKey, MultibandTile>> mappedRDD = new OpenEOProcesses().mapInstantToInterval(new ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>>(spacetimeDataCube.rdd(), datacube.metadata()), Arrays.asList("2017-01-01T00:00:00Z", "2017-01-30T00:00:00Z","2017-01-30T00:00:00Z", "2017-03-30T00:00:00Z"), Arrays.asList("2017-01-01T00:00:00Z","2017-01-15T00:00:00Z"));
        List<Tuple2<SpaceTimeKey, MultibandTile>> map = JavaPairRDD$.MODULE$.fromJavaRDD(mappedRDD.toJavaRDD()).collect();
        map.forEach(spaceTimeKey -> System.out.println("spaceTimeKey = " + spaceTimeKey._1.time()));
        System.out.println("map = " + map);
        assertEquals(2, map.stream().filter(tuple -> tuple._1().time().equals(ZonedDateTime.parse("2017-01-01T00:00:00Z"))).count());
        assertEquals(1, map.stream().filter(tuple -> tuple._1().time().equals(ZonedDateTime.parse("2017-01-15T00:00:00Z"))).count());
    }

    @Test
    public void testRasterMask() {
        DataType celltype = CellType$.MODULE$.fromName("int8raw").withDefaultNoData();

        MutableArrayTile zeroTile = new ByteConstantTile((byte)0,256,256, (ByteCells) celltype).mutable();
        zeroTile.set(0,0,1);
        zeroTile.set(0,1,ByteConstantNoDataCellType.noDataValue());

        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> tileLayerRDD = tileToSpaceTimeDataCube(zeroTile);

        MutableArrayTile maskTile = new ByteConstantTile((byte)0,256,256, (ByteCells) celltype).mutable();
        maskTile.set(0,0,0);
        maskTile.set(0,1,1);
        maskTile.set(0,2,1);

        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> maskRDD = tileToSpaceTimeDataCube(maskTile);
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> masked = new OpenEOProcesses().rasterMask(tileLayerRDD, maskRDD, 10.0);

        JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(masked.toJavaRDD());
        assertFalse(result.isEmpty());
        Map<SpaceTimeKey, MultibandTile> tiles = result.collectAsMap();
        for (Map.Entry<SpaceTimeKey, MultibandTile> tileEntry : tiles.entrySet()) {
            Tile tile = tileEntry.getValue().band(0);
            assertEquals(1,tile.get(0,0));
            //get method applies a conversion to int, also nodata is converted
            assertEquals(IntConstantNoDataCellType.noDataValue(),tile.get(0,1));
            assertEquals(10,tile.get(0,2));
        }


    }

    @Test
    public void testMergeCubes() {
        DataType celltype = CellType$.MODULE$.fromName("int8raw").withDefaultNoData();
        MutableArrayTile zeroTile = new ByteConstantTile((byte)0,256,256, (ByteCells) celltype).mutable();
        zeroTile.set(0,0,1);
        zeroTile.set(0,1,ByteConstantNoDataCellType.noDataValue());

        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> tileLayerRDD = tileToSpaceTimeDataCube(zeroTile);

        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> merged = new OpenEOProcesses().mergeCubes(tileLayerRDD, tileLayerRDD,null);

        MultibandTile firstTile = merged.toJavaRDD().take(1).get(0)._2();
        System.out.println("firstTile = " + firstTile);
        assertEquals(4,firstTile.bandCount());
        assertEquals(zeroTile,firstTile.band(0));
        assertEquals(zeroTile,firstTile.band(2));



        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> mergedOr = new OpenEOProcesses().mergeCubes(tileLayerRDD, tileLayerRDD,"sum");
        MultibandTile firstTileSum = mergedOr.toJavaRDD().take(1).get(0)._2();
        System.out.println("firstTileOr = " + firstTileSum);
        assertEquals(2,firstTileSum.bandCount());
        assertEquals(2, firstTileSum.band(0).get(0, 0));

        assertTrue(firstTileSum.band(1).isNoDataTile());
    }

    @Test
    public void testMergeCubesBadOperator() {
        DataType celltype = CellType$.MODULE$.fromName("int8raw").withDefaultNoData();
        MutableArrayTile zeroTile = new ByteConstantTile((byte)0,256,256, (ByteCells) celltype).mutable();
        zeroTile.set(0,0,1);
        zeroTile.set(0,1,ByteConstantNoDataCellType.noDataValue());

        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> tileLayerRDD = tileToSpaceTimeDataCube(zeroTile);

        try {
            new OpenEOProcesses().mergeCubes(tileLayerRDD, tileLayerRDD, "unsupported");
            fail("Should have thrown an exception.");
        } catch (UnsupportedOperationException e) {

        }

    }

    @Test
    public void testWriteCatalog() {
        Tile tile10 = new ByteConstantTile((byte)10,256,256, (ByteCells) CellType$.MODULE$.fromName("int8raw"));
        Tile tile5 = new ByteConstantTile((byte)5,256,256, (ByteCells) CellType$.MODULE$.fromName("int8raw"));
        RDD<Tuple2<SpatialKey, MultibandTile>> datacube = TileLayerRDDBuilders$.MODULE$.createMultibandTileLayerRDD(SparkContext.getOrCreate(), new ArrayMultibandTile(new Tile[]{tile10,tile5}), new TileLayout(1, 1, 256, 256));
        new OpenEOProcesses().write_geotiffs(datacube,"catalog",14);
    }

    static ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> tileToSpaceTimeDataCube(Tile zeroTile) {

        MutableArrayTile emptyTile = ArrayTile$.MODULE$.empty(zeroTile.cellType(), zeroTile.cols(), zeroTile.rows());

        ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpatialKey>> datacube = (ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpatialKey>>) TileLayerRDDBuilders$.MODULE$.createMultibandTileLayerRDD(SparkContext.getOrCreate(), new ArrayMultibandTile(new Tile[]{zeroTile,emptyTile}), new TileLayout(1, 1, zeroTile.cols(), zeroTile.rows()));
        final ZonedDateTime minDate = ZonedDateTime.parse("2017-01-01T00:00:00Z");
        final ZonedDateTime maxDate = ZonedDateTime.parse("2018-01-15T00:00:00Z");
        JavaPairRDD<SpaceTimeKey, MultibandTile> spacetimeDataCube = JavaPairRDD$.MODULE$.fromJavaRDD(datacube.toJavaRDD()).flatMapToPair(spatialKeyMultibandTileTuple2 -> {

            return Arrays.asList(
                    Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(minDate)), spatialKeyMultibandTileTuple2._2),
                    Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(ZonedDateTime.parse("2017-01-15T00:00:00Z"))), spatialKeyMultibandTileTuple2._2),
                    Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(ZonedDateTime.parse("2017-02-01T00:00:00Z"))), spatialKeyMultibandTileTuple2._2),
                    Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(maxDate)), spatialKeyMultibandTileTuple2._2)
            ).iterator();
        });
        TileLayerMetadata<SpatialKey> m = datacube.metadata();
        Bounds<SpatialKey> bounds = m.bounds();

        SpaceTimeKey minKey = SpaceTimeKey.apply(bounds.get().minKey(), TemporalKey.apply(minDate));
        SpaceTimeKey maxKey = SpaceTimeKey.apply(bounds.get().maxKey(), TemporalKey.apply(maxDate));
        KeyBounds<SpaceTimeKey> updatedKeyBounds = new KeyBounds<>(minKey,maxKey);
        TileLayerMetadata<SpaceTimeKey> metadata = new TileLayerMetadata<>(m.cellType(), m.layout(), m.extent(),m.crs(), updatedKeyBounds);

        return (ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>>) new ContextRDD(spacetimeDataCube.rdd(), metadata);
    }


}
