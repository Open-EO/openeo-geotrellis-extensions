package org.openeo.geotrellis;

import geotrellis.raster.*;
import geotrellis.spark.SpatialKey;
import geotrellis.spark.testkit.TileLayerRDDBuilders$;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

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
}
