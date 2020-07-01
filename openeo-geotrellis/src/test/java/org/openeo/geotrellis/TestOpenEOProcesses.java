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
import java.util.stream.Collectors;

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
        OpenEOProcessScriptBuilder processBuilder = TestOpenEOProcessScriptBuilder.createNormalizedDifferenceProcess10AddXY();
        Tile tile0 = new DoubleConstantTile(3.0,256,256, (DoubleCells) CellType$.MODULE$.fromName("float64"));
        Tile tile1 = new DoubleConstantTile(1.0,256,256, (DoubleCells) CellType$.MODULE$.fromName("float64"));
        RDD<Tuple2<SpatialKey, MultibandTile>> datacube = TileLayerRDDBuilders$.MODULE$.createMultibandTileLayerRDD(SparkContext.getOrCreate(), new ArrayMultibandTile(new Tile[]{tile0,tile1}), new TileLayout(1, 1, 256, 256));
        ClassTag<SpatialKey> tag = scala.reflect.ClassTag$.MODULE$.apply(SpatialKey.class);
        RDD<Tuple2<SpatialKey, MultibandTile>> ndviDatacube = new OpenEOProcesses().<SpatialKey>mapBandsGeneric(datacube, processBuilder,tag);
        List<Tuple2<SpatialKey, MultibandTile>> result = ndviDatacube.toJavaRDD().collect();
        System.out.println("result = " + result);
        assertEquals(1, result.get(0)._2().bandCount());
        double[] doubles = result.get(0)._2().band(0).toArrayDouble();
        assertEquals(0.5,doubles[0],0.0);
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
    public void testMergeCubesBasic() {
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
    }

    @Test
    public void testMergeCubesSumOperator() {
        DataType celltype = CellType$.MODULE$.fromName("int8raw").withDefaultNoData();
        MutableArrayTile zeroTile = new ByteConstantTile((byte) 0, 256, 256, (ByteCells) celltype).mutable();
        zeroTile.set(0, 0, 1);
        zeroTile.set(0, 1, ByteConstantNoDataCellType.noDataValue());
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> tileLayerRDD = tileToSpaceTimeDataCube(zeroTile);

        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> mergedOr = new OpenEOProcesses().mergeCubes(tileLayerRDD, tileLayerRDD,"sum");
        MultibandTile firstTileSum = mergedOr.toJavaRDD().take(1).get(0)._2();
        System.out.println("firstTileOr = " + firstTileSum);
        assertEquals(2,firstTileSum.bandCount());
        assertEquals(2, firstTileSum.band(0).get(0, 0));
        assertTrue(firstTileSum.band(1).isNoDataTile());
    }

    @Test
    public void testMergeCubeConcat() {
        // Set up
        ByteArrayTile band1 = ByteArrayTile.fill((byte) 2, 256, 256);
        ByteArrayTile band2 = ByteArrayTile.fill((byte) 3, 256, 256);
        ArrayTile band3 = ByteArrayTile.fill((byte) 5, 256, 256).convert(CellType.fromName("uint16"));
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> cube1 = buildSpatioTemporalDataCube(
                Arrays.asList(band1, band2),
                Arrays.asList("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z")
        );
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> cube2 = buildSpatioTemporalDataCube(
                Arrays.asList(band3, band3, band3),
                Arrays.asList("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z")
        );

        // Do merge
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> merged = new OpenEOProcesses().mergeCubes(cube1, cube2, null);

        // Check result
        List<TemporalKey> mergedTimes = merged.toJavaRDD().map(p -> p._1.temporalKey()).collect();
        assertEquals(2, mergedTimes.size());
        for (Tuple2<SpaceTimeKey, MultibandTile> item : merged.toJavaRDD().collect()) {
            assertEquals(5, item._2.bandCount());
            assertEquals(2, item._2.band(0).get(0, 0));
            assertEquals(3, item._2.band(1).get(0, 0));
            assertEquals(5, item._2.band(2).get(0, 0));
            assertEquals(5, item._2.band(3).get(0, 0));
            assertEquals(5, item._2.band(4).get(0, 0));
        }
        assertEquals(CellType.fromName("uint16"),merged.metadata().cellType());
    }


    @Test
    public void testMergeCubeTemporalDisjointNoOp() {
        // Set up
        ByteArrayTile band1 = ByteArrayTile.fill((byte) 2, 256, 256);
        ByteArrayTile band2 = ByteArrayTile.fill((byte) 3, 256, 256);
        ByteArrayTile band3 = ByteArrayTile.fill((byte) 5, 256, 256);
        ByteArrayTile band4 = ByteArrayTile.fill((byte) 8, 256, 256);
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> cube1 = buildSpatioTemporalDataCube(
                Arrays.asList(band1, band2),
                Arrays.asList("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z", "2020-03-03T00:00:00Z")
        );
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> cube2 = buildSpatioTemporalDataCube(
                Arrays.asList(band3, band4),
                Arrays.asList("2020-11-11T00:00:00Z", "2020-12-12T00:00:00Z")
        );

        // Do merge
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> merged = new OpenEOProcesses().mergeCubes(cube1, cube2, null);

        // Check result
        List<TemporalKey> mergedTimes = merged.toJavaRDD().map(p -> p._1.temporalKey()).collect();
        assertEquals(5, mergedTimes.size());
        for (Tuple2<SpaceTimeKey, MultibandTile> item : merged.toJavaRDD().collect()) {
            assertEquals(4, item._2.bandCount());
            if (item._1.temporalKey().time().isBefore(ZonedDateTime.parse("2020-10-01T00:00:00Z"))) {
                // time range with left part bands
                assertEquals(2, item._2.band(0).get(0, 0));
                assertEquals(3, item._2.band(1).get(0, 0));
                assertTrue(item._2.band(2).isNoDataTile());
                assertTrue(item._2.band(3).isNoDataTile());
            } else {
                // time range with right part bands
                assertTrue(item._2.band(0).isNoDataTile());
                assertTrue(item._2.band(1).isNoDataTile());
                assertEquals(5, item._2.band(2).get(0, 0));
                assertEquals(8, item._2.band(3).get(0, 0));
            }
        }
    }

    @Test
    public void testMergeCubePartialOverlapDifference() {
        // Set up
        ByteArrayTile band1 = ByteArrayTile.fill((byte) 2, 256, 256);
        ByteArrayTile band2 = ByteArrayTile.fill((byte) 3, 256, 256);
        ByteArrayTile band3 = ByteArrayTile.fill((byte) 5, 256, 256);
        ByteArrayTile band4 = ByteArrayTile.fill((byte) 8, 256, 256);
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> cube1 = buildSpatioTemporalDataCube(
                Arrays.asList(band1, band2),
                Arrays.asList("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z")
        );
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> cube2 = buildSpatioTemporalDataCube(
                Arrays.asList(band3, band4),
                Arrays.asList("2020-02-02T00:00:00Z", "2020-03-03T00:00:00Z")
        );

        // Do merge
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> merged = new OpenEOProcesses().mergeCubes(cube1, cube2, "subtract");

        // Check result
        List<TemporalKey> mergedTimes = merged.toJavaRDD().map(p -> p._1.temporalKey()).collect();
        assertEquals(3, mergedTimes.size());
        for (Tuple2<SpaceTimeKey, MultibandTile> item : merged.toJavaRDD().collect()) {
            assertEquals(2, item._2.bandCount());
            int month = item._1.temporalKey().time().getMonthValue();
            if (month == 1) {
                assertEquals(2, item._2.band(0).get(0, 0));
                assertEquals(3, item._2.band(1).get(0, 0));
            } else if (month == 2){
                assertEquals(-3, item._2.band(0).get(0, 0));
                assertEquals(-5, item._2.band(1).get(0, 0));
            } else {
                assertEquals(5, item._2.band(0).get(0, 0));
                assertEquals(8, item._2.band(1).get(0, 0));
            }
        }
    }

    @Test
    public void testMergeCubeFullOverlapNoOp() {
        // Set up
        ByteArrayTile band1 = ByteArrayTile.fill((byte) 1, 256, 256);
        ByteArrayTile band2 = ByteArrayTile.fill((byte) 2, 256, 256);
        ByteArrayTile band3 = ByteArrayTile.fill((byte) 3, 256, 256);
        ByteArrayTile band4 = ByteArrayTile.fill((byte) 4, 256, 256);
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> cube1 = buildSpatioTemporalDataCube(
                Arrays.asList(band1, band2),
                Arrays.asList("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z")
        );
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> cube2 = buildSpatioTemporalDataCube(
                Arrays.asList(band3, band4),
                Arrays.asList("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z")
        );

        // Do merge
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> merged = new OpenEOProcesses().mergeCubes(cube1, cube2, null);

        // Check result
        List<TemporalKey> mergedTimes = merged.toJavaRDD().map(p -> p._1.temporalKey()).collect();
        assertEquals(2, mergedTimes.size());
        for (Tuple2<SpaceTimeKey, MultibandTile> item : merged.toJavaRDD().collect()) {
            assertEquals(4, item._2.bandCount());
            assertEquals(1, item._2.band(0).get(0, 0));
            assertEquals(2, item._2.band(1).get(0, 0));
            assertEquals(3, item._2.band(2).get(0, 0));
            assertEquals(4, item._2.band(3).get(0, 0));
        }
    }

    @Test
    public void testMergeCubeFullOverlapDifference() {
        // Set up
        ByteArrayTile band1 = ByteArrayTile.fill((byte) 2, 256, 256);
        ByteArrayTile band2 = ByteArrayTile.fill((byte) 3, 256, 256);
        ByteArrayTile band3 = ByteArrayTile.fill((byte) 5, 256, 256);
        ByteArrayTile band4 = ByteArrayTile.fill((byte) 8, 256, 256);
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> cube1 = buildSpatioTemporalDataCube(
                Arrays.asList(band1, band2),
                Arrays.asList("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z")
        );
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> cube2 = buildSpatioTemporalDataCube(
                Arrays.asList(band3, band4),
                Arrays.asList("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z")
        );

        // Do merge
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> merged = new OpenEOProcesses().mergeCubes(cube1, cube2, "subtract");

        // Check result
        List<TemporalKey> mergedTimes = merged.toJavaRDD().map(p -> p._1.temporalKey()).collect();
        assertEquals(2, mergedTimes.size());
        for (Tuple2<SpaceTimeKey, MultibandTile> item : merged.toJavaRDD().collect()) {
            assertEquals(2, item._2.bandCount());
            assertEquals(-3, item._2.band(0).get(0, 0));
            assertEquals(-5, item._2.band(1).get(0, 0));
        }
    }

    @Test
    public void testMergeCubeOverlapBandMismatch() {
        // Set up
        ByteArrayTile band1 = ByteArrayTile.fill((byte) 2, 256, 256);
        ByteArrayTile band2 = ByteArrayTile.fill((byte) 3, 256, 256);
        ByteArrayTile band3 = ByteArrayTile.fill((byte) 5, 256, 256);
        ByteArrayTile band4 = ByteArrayTile.fill((byte) 8, 256, 256);
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> cube1 = buildSpatioTemporalDataCube(
                Arrays.asList(band1, band2),
                Arrays.asList("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z")
        );
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> cube2 = buildSpatioTemporalDataCube(
                Arrays.asList(band3, band4),
                Arrays.asList("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z")
        );

        // Do merge
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> merged = new OpenEOProcesses().mergeCubes(cube1, cube2, "subtract");

        // Check result
        List<TemporalKey> mergedTimes = merged.toJavaRDD().map(p -> p._1.temporalKey()).collect();
        assertEquals(2, mergedTimes.size());
        for (Tuple2<SpaceTimeKey, MultibandTile> item : merged.toJavaRDD().collect()) {
            assertEquals(2, item._2.bandCount());
            assertEquals(-3, item._2.band(0).get(0, 0));
            assertEquals(-5, item._2.band(1).get(0, 0));
        }
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
        new OpenEOProcesses().write_geotiffs(datacube,"tmpcatalog",14);
    }

    static ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> tileToSpaceTimeDataCube(Tile zeroTile) {

        MutableArrayTile emptyTile = ArrayTile$.MODULE$.empty(zeroTile.cellType(), ((Integer) zeroTile.cols()), ((Integer) zeroTile.rows()));

        ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpatialKey>> datacube = (ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpatialKey>>) TileLayerRDDBuilders$.MODULE$.createMultibandTileLayerRDD(SparkContext.getOrCreate(), new ArrayMultibandTile(new Tile[]{zeroTile,emptyTile}), new TileLayout(1, 1, ((Integer) zeroTile.cols()), ((Integer) zeroTile.rows())));
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

    static ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> buildSpatioTemporalDataCube(List<Tile> tiles, List<String> dates) {
        ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpatialKey>> cubeXYB = (ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpatialKey>>) TileLayerRDDBuilders$.MODULE$.createMultibandTileLayerRDD(
                SparkContext.getOrCreate(),
                new ArrayMultibandTile((Tile[]) tiles.toArray()),
                new TileLayout(1, 1, ((Integer) tiles.get(0).cols()), ((Integer) tiles.get(0).rows()))
        );
        List<ZonedDateTime> times = dates.stream().map(ZonedDateTime::parse).collect(Collectors.toList());
        JavaPairRDD<SpaceTimeKey, MultibandTile> cubeXYTB = JavaPairRDD$.MODULE$.fromJavaRDD(cubeXYB.toJavaRDD()).flatMapToPair(pair -> {
            return times.stream().map(time ->
                    Tuple2.apply(SpaceTimeKey.apply(pair._1, TemporalKey.apply(time)), pair._2)
            ).iterator();
        });
        TileLayerMetadata<SpatialKey> md = cubeXYB.metadata();
        Bounds<SpatialKey> bounds = md.bounds();
        SpaceTimeKey minKey = SpaceTimeKey.apply(bounds.get().minKey(), TemporalKey.apply(times.get(0)));
        SpaceTimeKey maxKey = SpaceTimeKey.apply(bounds.get().maxKey(), TemporalKey.apply(times.get(times.size() - 1)));
        TileLayerMetadata<SpaceTimeKey> metadata = new TileLayerMetadata<>(md.cellType(), md.layout(), md.extent(), md.crs(), new KeyBounds<SpaceTimeKey>(minKey, maxKey));

        return (ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>>) new ContextRDD(cubeXYTB.rdd(), metadata);
    }


}
