package org.openeo.geotrellis;


import geotrellis.layer.*;
import geotrellis.raster.*;
import geotrellis.spark.ContextRDD;
import geotrellis.spark.partition.SpacePartitioner;
import geotrellis.spark.testkit.TileLayerRDDBuilders$;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaPairRDD$;
import org.apache.spark.rdd.RDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.math.BigInt;
import scala.reflect.ClassTag;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import static org.junit.Assert.*;
import static org.openeo.geotrellis.TestOpenEOProcessScriptBuilder.createMax;
import static org.openeo.geotrellis.TestOpenEOProcessScriptBuilder.createStandardDeviation;

public class TestOpenEOProcesses {

    @BeforeClass
    public static void sparkContext() {

        SparkConf conf = new SparkConf();
        conf.setAppName("OpenEOTest");
        conf.setMaster("local[1]");
        conf.set("spark.driver.bindAddress", "127.0.0.1");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkContext.getOrCreate(conf);


    }

    @AfterClass
    public static void shutDownSparkContext() {
        SparkContext.getOrCreate().stop();
    }


    @DisplayName("Test combining all bands.")
    @Test
    public void testMapBands() {
        OpenEOProcessScriptBuilder processBuilder = TestOpenEOProcessScriptBuilder.createNormalizedDifferenceProcess10AddXY();
        assertEquals(FloatConstantNoDataCellType$.MODULE$,processBuilder.getOutputCellType());
        Tile tile0 = new UByteConstantTile((byte)3,256,256, (UByteCells)  CellType$.MODULE$.fromName("uint8"));
        Tile tile1 = new UByteConstantTile((byte)1,256,256, (UByteCells) CellType$.MODULE$.fromName("uint8"));
        RDD<Tuple2<SpatialKey, MultibandTile>> datacube = TileLayerRDDBuilders$.MODULE$.createMultibandTileLayerRDD(SparkContext.getOrCreate(), new ArrayMultibandTile(new Tile[]{tile0,tile1}), new TileLayout(1, 1, 256, 256));
        ClassTag<SpatialKey> tag = scala.reflect.ClassTag$.MODULE$.apply(SpatialKey.class);
        ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpatialKey>> ndviDatacube = (ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpatialKey>>) new OpenEOProcesses().<SpatialKey>mapBandsGeneric(datacube, processBuilder, new HashMap<>(), tag);
        assertEquals(FloatConstantNoDataCellType$.MODULE$,ndviDatacube.metadata().cellType());
        List<Tuple2<SpatialKey, MultibandTile>> result = ndviDatacube.toJavaRDD().collect();
        assertEquals(FloatConstantNoDataCellType$.MODULE$,result.get(0)._2.cellType());
        System.out.println("result = " + result);
        assertEquals(1, result.get(0)._2().bandCount());
        double[] doubles = result.get(0)._2().band(0).toArrayDouble();
        assertEquals(0.5,doubles[0],0.0);
    }

    @Test
    public void testMapToIntervals() {
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1 = createSpacetimeLayer();
        RDD<Tuple2<SpaceTimeKey, MultibandTile>> mappedRDD = new OpenEOProcesses().mapInstantToInterval(datacube1, Arrays.asList("2017-01-01T00:00:00Z", "2017-01-30T00:00:00Z","2017-01-30T00:00:00Z", "2017-03-30T00:00:00Z"), Arrays.asList("2017-01-01T00:00:00Z","2017-01-15T00:00:00Z"));
        List<Tuple2<SpaceTimeKey, MultibandTile>> map = JavaPairRDD$.MODULE$.fromJavaRDD(mappedRDD.toJavaRDD()).collect();
        map.forEach(spaceTimeKey -> System.out.println("spaceTimeKey = " + spaceTimeKey._1.time()));
        System.out.println("map = " + map);
        assertEquals(2, map.stream().filter(tuple -> tuple._1().time().equals(ZonedDateTime.parse("2017-01-01T00:00:00Z"))).count());
        assertEquals(1, map.stream().filter(tuple -> tuple._1().time().equals(ZonedDateTime.parse("2017-01-15T00:00:00Z"))).count());
    }

    @Test
    public void testAggregateTemporal() {

        ByteArrayTile band1 = ByteArrayTile.fill((byte) -10, 256, 256);
        ByteArrayTile band2 = ByteArrayTile.fill((byte) 4, 256, 256);
        ByteArrayTile band3 = ByteArrayTile.fill((byte) 5, 256, 256);
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1 =LayerFixtures.buildSingleBandSpatioTemporalDataCube(Arrays.asList(band1,ByteArrayTile.empty(256,256), band2,band3), JavaConversions.asScalaBuffer(Arrays.asList("2017-01-03T00:00:00Z", "2017-01-30T00:00:00Z","2017-01-30T00:00:00Z", "2017-03-30T00:00:00Z")));
        OpenEOProcessScriptBuilder processBuilder = TestOpenEOProcessScriptBuilder.createMedian(true,datacube1.metadata().cellType());

        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> mappedRDD = (ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>>) new OpenEOProcesses().aggregateTemporal(datacube1, Arrays.asList("2017-01-01T00:00:00Z", "2017-01-30T00:00:00Z","2017-01-30T00:00:00Z", "2017-03-30T00:00:00Z"), Arrays.asList("2017-01-01T00:00:00Z","2017-01-15T00:00:00Z"),processBuilder, Collections.EMPTY_MAP);

        //bounds needs to be updated to reflect the new bounds
        assertEquals(new KeyBounds<SpaceTimeKey>(new SpaceTimeKey(0,0,1483228800000L),new SpaceTimeKey(0,0,1484438400000L)),mappedRDD.metadata().bounds());
        assertTrue(mappedRDD.partitioner().get() instanceof SpacePartitioner);
        Map<SpaceTimeKey, MultibandTile> map = JavaPairRDD$.MODULE$.fromJavaRDD(mappedRDD.toJavaRDD()).collectAsMap();
        //map.forEach(spaceTimeKey -> System.out.println("spaceTimeKey = " + spaceTimeKey._1.time()));
        System.out.println("map = " + map);
        assertEquals(1, map.entrySet().stream().filter(tuple -> tuple.getKey().time().equals(ZonedDateTime.parse("2017-01-01T00:00:00Z"))).count());
        assertEquals(1, map.entrySet().stream().filter(tuple -> tuple.getKey().time().equals(ZonedDateTime.parse("2017-01-15T00:00:00Z"))).count());

        Set<DataType> dataTypes = map.entrySet().stream().map(t -> t.getValue().cellType()).collect(Collectors.toSet());
        assertEquals(1, dataTypes.size());
        assertEquals(ByteConstantNoDataCellType$.MODULE$,dataTypes.iterator().next());

        assertEquals(-10, map.entrySet().stream().filter(tuple -> tuple.getKey().time().equals(ZonedDateTime.parse("2017-01-01T00:00:00Z"))).findFirst().get().getValue().band(0).get(0,0));
        assertEquals(4, map.entrySet().stream().filter(tuple -> tuple.getKey().time().equals(ZonedDateTime.parse("2017-01-15T00:00:00Z"))).findFirst().get().getValue().band(0).get(0,0));

    }


    private ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> createSpacetimeLayer() {
        Tile zeroTile = new ByteConstantTile((byte)0,256,256, (ByteCells) CellType$.MODULE$.fromName("int8raw"));
        ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = (ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>>) TileLayerRDDBuilders$.MODULE$.createMultibandTileLayerRDD(SparkContext.getOrCreate(), new ArrayMultibandTile(new Tile[]{zeroTile}), new TileLayout(1, 1, 256, 256));
        JavaPairRDD<SpaceTimeKey, MultibandTile> spacetimeDataCube = JavaPairRDD$.MODULE$.fromJavaRDD(datacube.toJavaRDD()).flatMapToPair(spatialKeyMultibandTileTuple2 -> Arrays.asList(
                Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(ZonedDateTime.parse("2017-01-01T00:00:00Z"))), spatialKeyMultibandTileTuple2._2),
                Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(ZonedDateTime.parse("2017-01-15T00:00:00Z"))), spatialKeyMultibandTileTuple2._2),
                Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(ZonedDateTime.parse("2017-02-01T00:00:00Z"))), spatialKeyMultibandTileTuple2._2),
                Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(ZonedDateTime.parse("2018-01-15T00:00:00Z"))), spatialKeyMultibandTileTuple2._2)
        ).iterator());
        //JavaConverters.
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1 = new ContextRDD<>(spacetimeDataCube.rdd(), datacube.metadata());
        return datacube1;
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
        RDD<Tuple2<SpaceTimeKey, MultibandTile>> masked = new OpenEOProcesses().rasterMask(tileLayerRDD, maskRDD, 10.0);

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
    public void testWriteCatalog() {
        Tile tile10 = new ByteConstantTile((byte)10,256,256, (ByteCells) CellType$.MODULE$.fromName("int8raw"));
        Tile tile5 = new ByteConstantTile((byte)5,256,256, (ByteCells) CellType$.MODULE$.fromName("int8raw"));
        RDD<Tuple2<SpatialKey, MultibandTile>> datacube = TileLayerRDDBuilders$.MODULE$.createMultibandTileLayerRDD(SparkContext.getOrCreate(), new ArrayMultibandTile(new Tile[]{tile10,tile5}), new TileLayout(1, 1, 256, 256));
        new OpenEOProcesses().write_geotiffs(datacube,"tmpcatalog",14);
    }

    public static ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> tileToSpaceTimeDataCube(Tile zeroTile) {

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

    @Test
    public void testApplyTimeDimension() {
        ByteArrayTile band1 = ByteArrayTile.fill((byte) -10, 256, 256);
        ByteArrayTile band2 = ByteArrayTile.fill((byte) 4, 256, 256);
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1 =LayerFixtures.buildSingleBandSpatioTemporalDataCube(Arrays.asList(band1,ByteArrayTile.empty(256,256), band2), JavaConversions.asScalaBuffer(Arrays.asList("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z","2020-02-03T00:00:00Z")));
        OpenEOProcessScriptBuilder processBuilder = TestOpenEOProcessScriptBuilder.createArrayInterpolateLinear(datacube1.metadata().cellType());
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> result = ((ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>>)new OpenEOProcesses().applyTimeDimension(datacube1, processBuilder, new HashMap<>()));
        List<Tuple2<String,MultibandTile>> results = JavaPairRDD.fromJavaRDD(result.toJavaRDD()).map(spaceTimeKeyMultibandTileTuple2 -> new Tuple2<String,MultibandTile>(spaceTimeKeyMultibandTileTuple2._1.time().toString(),spaceTimeKeyMultibandTileTuple2._2)).collect();
        MultibandTile interpolatedTile = results.stream().filter(tuple2 -> tuple2._1.equals("2020-02-02T00:00Z")).collect(Collectors.toList()).get(0)._2;
        assertEquals(IntConstantNoDataCellType$.MODULE$,result.metadata().cellType());
        assertEquals(IntConstantNoDataCellType$.MODULE$,interpolatedTile.cellType());
        assertEquals(-3, interpolatedTile.band(0).get(0,0));

    }


    @Test
    public void testCompositeAndInterpolate() {
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1 = LayerFixtures.sentinel2B04Layer();
        compositeAndInterpolate(datacube1);
    }

    @Test
    public void testCompositeAndInterpolateSparse() {
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1 = LayerFixtures.sentinel2B04LayerSparse();
        compositeAndInterpolate(datacube1);
    }

    private void compositeAndInterpolate(ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1) {
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> compositedCube = composite(datacube1);

        List<SpaceTimeKey> keys = compositedCube.toJavaRDD().map(v1 -> v1._1).collect();
        Option<SpaceTimeKey[]> partitionerKeys = new OpenEOProcesses().findPartitionerKeys(compositedCube);
        if (partitionerKeys.isDefined()) {
            assertArrayEquals(keys.toArray(),Arrays.stream(partitionerKeys.get()).toArray());
        }
        
        //Partitioner p = compositedCube.partitioner().get();
        //keys.forEach(spaceTimeKey -> System.out.println("p.getPartition(spaceTimeKey) = " + p.getPartition(spaceTimeKey)) );

        OpenEOProcessScriptBuilder processBuilder = TestOpenEOProcessScriptBuilder.createArrayInterpolateLinear(compositedCube.metadata().cellType());
        RDD<Tuple2<SpaceTimeKey, MultibandTile>> result = new OpenEOProcesses().applyTimeDimension(compositedCube, processBuilder, new HashMap<>());

        //int[] compositedPixel = OpenEOProcessesSpec.getPixel(compositedCube);
        int[] interpolatedPixel = OpenEOProcessesSpec.getPixel(result);

        int noData = Integer.MIN_VALUE;
        assertArrayEquals(new int[]{noData,noData,noData,noData,290,317,346,375,405},interpolatedPixel);
    }

    @Test
    public void testApplyTimeDimensionToBandB04() {

        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1 = LayerFixtures.sentinel2B04Layer();

        RDD<Tuple2<SpaceTimeKey, MultibandTile>> compositedCube = composite(datacube1);


        OpenEOProcessScriptBuilder featureEngineeringProcess = TestOpenEOProcessScriptBuilder.createFeatureEngineering();
        assertEquals(FloatConstantNoDataCellType$.MODULE$,featureEngineeringProcess.resultingDataType());
        HashMap<String, Object> context = new HashMap<>();
        //context.put("TileSize",64);
        ContextRDD<SpatialKey, MultibandTile,TileLayerMetadata<SpatialKey>> result = ((ContextRDD<SpatialKey, MultibandTile,TileLayerMetadata<SpatialKey>>)new OpenEOProcesses().applyTimeDimensionTargetBands(new ContextRDD<>(compositedCube,datacube1.metadata()), featureEngineeringProcess, context));
        assertTrue(result.partitioner().get() instanceof SpacePartitioner);
        assertEquals(FloatConstantNoDataCellType$.MODULE$,result.metadata().cellType());

        BigInt p1 = ((SpacePartitioner<SpatialKey>) result.partitioner().get()).index().toIndex(new SpatialKey(0, 0));
        BigInt p2 = ((SpacePartitioner<SpatialKey>) result.partitioner().get()).index().toIndex(new SpatialKey(1, 0));
        assertNotEquals("Partitions for neighbouring keys should be different",p1,p2);
        List<Tuple2<String,MultibandTile>> results = JavaPairRDD.fromJavaRDD(result.toJavaRDD()).map(spaceTimeKeyMultibandTileTuple2 -> new Tuple2<String,MultibandTile>(spaceTimeKeyMultibandTileTuple2._1.toString(),spaceTimeKeyMultibandTileTuple2._2)).collect();
        MultibandTile interpolatedTile = results.stream().collect(Collectors.toList()).get(0)._2;
        assertEquals(5,interpolatedTile.bandCount());
        List<Integer> bandValues = getPixel(interpolatedTile);
        System.out.println("bandValues = " + bandValues);

        int[] compositedPixel = OpenEOProcessesSpec.getPixel(compositedCube);

        DoubleStream doubleValues = Arrays.stream(compositedPixel)
                .filter(a -> a != Integer.MIN_VALUE)
                .mapToDouble(a -> a);

        double[] inputTSAsArray = doubleValues.toArray();
        double sd = new StandardDeviation().evaluate(inputTSAsArray);
        double p25 = new Percentile().evaluate(inputTSAsArray,25);
        double p50 = new Percentile().evaluate(inputTSAsArray,50);
        double p75 = new Percentile().evaluate(inputTSAsArray,75);

        assertArrayEquals(new Object[]{(int)p25, (int)p50, (int)p75, (int)sd, (int) Arrays.stream(inputTSAsArray).average().getAsDouble()}, bandValues.toArray());

    }

    @Test
    public void testApplyTimeDimensionToBandB04PreservesOrder() {

        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1 = LayerFixtures.sentinel2B04Layer();

        RDD<Tuple2<SpaceTimeKey, MultibandTile>> compositedCube = compositeByMonth(datacube1);


        OpenEOProcessScriptBuilder featureEngineeringProcess = TestOpenEOProcessScriptBuilder.createMonthSelection();
        RDD<Tuple2<SpatialKey, MultibandTile>> result = new OpenEOProcesses().applyTimeDimensionTargetBands(new ContextRDD<>(compositedCube,datacube1.metadata()), featureEngineeringProcess, new HashMap<>());
        List<Tuple2<String,MultibandTile>> results = JavaPairRDD.fromJavaRDD(result.toJavaRDD()).map(spaceTimeKeyMultibandTileTuple2 -> new Tuple2<String,MultibandTile>(spaceTimeKeyMultibandTileTuple2._1.toString(),spaceTimeKeyMultibandTileTuple2._2)).collect();
        MultibandTile interpolatedTile = results.stream().collect(Collectors.toList()).get(0)._2;
        assertEquals(2,interpolatedTile.bandCount());
        assertEquals(datacube1.metadata().cellType(),interpolatedTile.cellType());
        List<Integer> bandValues = getPixel(interpolatedTile);
        System.out.println("bandValues = " + bandValues);

        int[] compositedPixel = OpenEOProcessesSpec.getPixel(compositedCube);

        assertArrayEquals(new Object[]{ 377, 261}, bandValues.toArray());
        assertArrayEquals(new Object[]{ compositedPixel[3], compositedPixel[8]}, bandValues.toArray());

    }

    @Test
    public void testReduceTime() {
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1 = LayerFixtures.sentinel2B04Layer();
        reduceMaxTest(datacube1);
    }

    @Test
    public void testReduceTimeSparse() {
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1 = LayerFixtures.sentinel2B04LayerSparse();
        reduceMaxTest(datacube1);
    }
    private static void reduceMaxTest(ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1) {
        int[] compositedPixel = OpenEOProcessesSpec.getPixel(datacube1);
        int expectedMax = Arrays.stream(compositedPixel).max().getAsInt();

        ContextRDD<SpatialKey, MultibandTile,TileLayerMetadata<SpatialKey>> reduced =
                (ContextRDD<SpatialKey, MultibandTile,TileLayerMetadata<SpatialKey>>)new OpenEOProcesses().reduceTimeDimension(datacube1, createMax(datacube1.metadata().cellType()), Collections.emptyMap());
        assertEquals(datacube1.metadata().cellType(),reduced.metadata().cellType());
        Object shouldBeSingleTile = reduced.collect();
        assertEquals(1,((Tuple2<SpatialKey, MultibandTile>[])shouldBeSingleTile).length);
        MultibandTile maxTile = ((Tuple2<SpatialKey, MultibandTile>[])shouldBeSingleTile)[0]._2;
        assertEquals(expectedMax,maxTile.band(0).get(0,0));
        assertEquals(datacube1.metadata().cellType(),maxTile.cellType());
    }

    @Test
    public void testReduceTimeSD() {
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1 = LayerFixtures.sentinel2B04Layer();
        int[] compositedPixel = OpenEOProcessesSpec.getPixel(datacube1);
        //int expectedMax = Arrays.stream(compositedPixel).summaryStatistics();

        ContextRDD<SpatialKey, MultibandTile,TileLayerMetadata<SpatialKey>> reduced =
                (ContextRDD<SpatialKey, MultibandTile,TileLayerMetadata<SpatialKey>>)new OpenEOProcesses().reduceTimeDimension(datacube1, createStandardDeviation(true,datacube1.metadata().cellType()), Collections.emptyMap());
        assertEquals(FloatConstantNoDataCellType$.MODULE$,reduced.metadata().cellType());
        Object shouldBeSingleTile = reduced.collect();
        assertEquals(1,((Tuple2<SpatialKey, MultibandTile>[])shouldBeSingleTile).length);
        MultibandTile maxTile = ((Tuple2<SpatialKey, MultibandTile>[])shouldBeSingleTile)[0]._2;
        //assertEquals(expectedMax,maxTile.band(0).get(0,0));
        assertEquals(FloatConstantNoDataCellType$.MODULE$,maxTile.cellType());
    }

    private ContextRDD<SpaceTimeKey, MultibandTile,TileLayerMetadata<SpaceTimeKey>> composite(ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1) {
        OpenEOProcessScriptBuilder processBuilder = TestOpenEOProcessScriptBuilder.createMedian(true,datacube1.metadata().cellType());
        return (ContextRDD<SpaceTimeKey, MultibandTile,TileLayerMetadata<SpaceTimeKey>>) new OpenEOProcesses().aggregateTemporal(datacube1,
                Arrays.asList("2019-01-01T00:00:00Z", "2019-01-11T00:00:00Z",
                        "2019-01-11T00:00:00Z", "2019-01-21T00:00:00Z",
                        "2019-01-21T00:00:00Z", "2019-02-01T00:00:00Z",//0-10 -> 0
                        "2019-02-01T00:00:00Z", "2019-02-11T00:00:00Z",//11-20 ->
                        "2019-02-11T00:00:00Z", "2019-02-21T00:00:00Z",//21-31 -> 25
                        "2019-02-21T00:00:00Z", "2019-03-01T00:00:00Z",//32-39 , 35, 37,
                        "2019-03-01T00:00:00Z", "2019-03-11T00:00:00Z",//40-50
                        "2019-03-11T00:00:00Z", "2019-03-21T00:00:00Z",//51-61 55, 60,
                        "2019-03-21T00:00:00Z", "2019-04-01T00:00:00Z"//62-72 67, 70,
                ), Arrays.asList("2019-01-01T00:00:00Z", "2019-01-11T00:00:00Z", "2019-01-21T00:00:00Z", "2019-02-01T00:00:00Z", "2019-02-11T00:00:00Z", "2019-02-21T00:00:00Z", "2019-03-01T00:00:00Z", "2019-03-11T00:00:00Z", "2019-03-21T00:00:00Z"), processBuilder, Collections.EMPTY_MAP);
    }

    private RDD<Tuple2<SpaceTimeKey, MultibandTile>> compositeByMonth(ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube1) {
        OpenEOProcessScriptBuilder processBuilder = TestOpenEOProcessScriptBuilder.createMedian(true,datacube1.metadata().cellType());
        return (RDD<Tuple2<SpaceTimeKey, MultibandTile>>) new OpenEOProcesses().aggregateTemporal(datacube1,
                Arrays.asList("2019-01-01T00:00:00Z", "2019-02-01T00:00:00Z",
                        "2019-02-01T00:00:00Z", "2019-03-01T00:00:00Z",
                        "2019-03-01T00:00:00Z", "2019-04-01T00:00:00Z",//0-10 -> 0
                        "2019-04-01T00:00:00Z", "2019-05-01T00:00:00Z",//11-20 ->
                        "2019-05-01T00:00:00Z", "2019-06-01T00:00:00Z",//21-31 -> 25
                        "2019-06-01T00:00:00Z", "2019-07-01T00:00:00Z",//32-39 , 35, 37,
                        "2019-07-01T00:00:00Z", "2019-08-01T00:00:00Z",//40-50
                        "2019-08-01T00:00:00Z", "2019-09-01T00:00:00Z",//51-61 55, 60,
                        "2019-09-01T00:00:00Z", "2019-10-01T00:00:00Z"//62-72 67, 70,
                ), Arrays.asList("2019-01-01T00:00:00Z", "2019-02-01T00:00:00Z", "2019-03-01T00:00:00Z", "2019-04-01T00:00:00Z", "2019-05-01T00:00:00Z", "2019-06-01T00:00:00Z", "2019-07-01T00:00:00Z", "2019-08-01T00:00:00Z", "2019-09-01T00:00:00Z"), processBuilder, Collections.EMPTY_MAP);
    }

    private List<Integer> getPixel(MultibandTile interpolatedTile) {
        return JavaConverters.seqAsJavaListConverter(interpolatedTile.bands()).asJava().stream().map(tile -> tile.get(0, 0)).collect(Collectors.toList());
    }
}
