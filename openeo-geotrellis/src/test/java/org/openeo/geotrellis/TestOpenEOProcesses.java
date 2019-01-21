package org.openeo.geotrellis;

import geotrellis.raster.*;
import geotrellis.spark.*;
import geotrellis.spark.testkit.TileLayerRDDBuilders$;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaPairRDD$;
import org.apache.spark.rdd.RDD;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class TestOpenEOProcesses {

    @BeforeClass
    public static void sparkContext() {

        SparkConf conf = new SparkConf();
        conf.setAppName("OpenEOTest");
        conf.setMaster("local[4]");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkContext.getOrCreate(conf);


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


    }
