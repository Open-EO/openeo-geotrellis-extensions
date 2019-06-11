package org.openeo.geotrellisaccumulo;

import geotrellis.raster.MultibandTile;
import geotrellis.spark.SpaceTimeKey;
import geotrellis.vector.Extent;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.immutable.Seq;

import static org.junit.Assert.assertFalse;

public class PyramidFactoryTest {

    @BeforeClass
    public static void sparkContext() {
        HdfsConfiguration config = new HdfsConfiguration();
        config.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(config);
        SparkConf conf = new SparkConf();
        conf.setAppName("PyramidFactoryTest");
        conf.setMaster("local[4]");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        SparkContext sc =SparkContext.getOrCreate(conf);
        //creating context may have screwed up security settings
        UserGroupInformation.setConfiguration(config);
    }
    @Test
    public void createPyramid() {


        PyramidFactory pyramidFactory = new PyramidFactory("hdp-accumulo-instance", "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181");
        Extent bbox = new Extent(652000, 5161000, 672000, 5181000);
        String srs = "EPSG:32632";

        //bbox = new Extent(10.5, 46.5, 11.4, 46.9);
        //srs = "EPSG:4326";
        Seq<Tuple2<Object, RDD<Tuple2<SpaceTimeKey, MultibandTile>>>> pyramid = pyramidFactory.pyramid_seq("PROBAV_L3_S10_TOC_NDVI_333M", bbox, srs, "2016-12-31T00:00:00Z", "2018-01-01T02:00:00Z");
        System.out.println("pyramid = " + pyramid);
        assertFalse(pyramid.apply(0)._2.isEmpty());

    }
}
