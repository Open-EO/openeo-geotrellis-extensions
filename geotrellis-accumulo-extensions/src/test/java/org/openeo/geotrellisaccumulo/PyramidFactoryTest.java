package org.openeo.geotrellisaccumulo;

import be.vito.eodata.geopysparkextensions.AccumuloDelegationTokenProvider;
import geotrellis.layer.SpaceTimeKey;
import geotrellis.proj4.CRS;
import geotrellis.proj4.LatLng$;
import geotrellis.raster.MultibandTile;
import geotrellis.vector.Extent;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;
import scala.collection.Seq;

import java.io.IOException;

import static org.junit.Assert.assertFalse;

public class PyramidFactoryTest {

    @BeforeClass
    public static void sparkContext() throws IOException {
        HdfsConfiguration config = new HdfsConfiguration();
        config.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(config);
        SparkConf conf = new SparkConf();
        conf.setAppName("PyramidFactoryTest");
        conf.setMaster("local[4]");
        conf.set("spark.driver.bindAddress", "127.0.0.1");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        SparkContext sc =SparkContext.getOrCreate(conf);
        //creating context may have screwed up security settings
        UserGroupInformation.setConfiguration(config);
        Credentials creds = new Credentials();
        new AccumuloDelegationTokenProvider().obtainCredentials(config, conf, creds);
        UserGroupInformation.getCurrentUser().addCredentials(creds);
    }

    @AfterClass
    public static void shutDownSparkContext() {
        SparkContext.getOrCreate().stop();
    }

    private PyramidFactory pyramidFactory() {
        return new PyramidFactory("hdp-accumulo-instance", "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181");
    }

    @Test
    public void createPyramid() {


        Extent bbox = new Extent(652000, 5161000, 672000, 5181000);
        String srs = "EPSG:32632";

        //bbox = new Extent(10.5, 46.5, 11.4, 46.9);
        //srs = "EPSG:4326";
        Seq<Tuple2<Object, RDD<Tuple2<SpaceTimeKey, MultibandTile>>>> pyramid = pyramidFactory().pyramid_seq("PROBAV_L3_S10_TOC_NDVI_333M_V3", bbox, srs, "2016-12-31T00:00:00Z", "2018-01-01T02:00:00Z");
        System.out.println("pyramid = " + pyramid);
        assertFalse(pyramid.apply(0)._2.isEmpty());

    }

    @Test
    public void createPyramidFromPolygons() throws Exception {
        Polygon polygon1 = (Polygon) package$.MODULE$.parseGeometry("{\"type\":\"Polygon\",\"coordinates\":[[[5.0761587693484875,51.21222494794898],[5.166854684377381,51.21222494794898],[5.166854684377381,51.268936260927404],[5.0761587693484875,51.268936260927404],[5.0761587693484875,51.21222494794898]]]}")._2();
        Polygon polygon2 = (Polygon) package$.MODULE$.parseGeometry("{\"type\":\"Polygon\",\"coordinates\":[[[3.043212890625,51.17934297928927],[3.087158203125,51.17934297928927],[3.087158203125,51.210324789481355],[3.043212890625,51.210324789481355],[3.043212890625,51.17934297928927]]]}")._2();

        MultiPolygon[] multiPolygons = new MultiPolygon[] {
                new MultiPolygon(new Polygon[]{polygon1}, new GeometryFactory()),
                new MultiPolygon(new Polygon[]{polygon2}, new GeometryFactory())
        };

        CRS polygons_crs = LatLng$.MODULE$;

        String startDate = "2017-11-01T00:00:00Z";
        String endDate = "2017-11-21T00:00:00Z";

        Seq<Tuple2<Object, RDD<Tuple2<SpaceTimeKey, MultibandTile>>>> pyramid =
                pyramidFactory().pyramid_seq("S2_FAPAR_V102_WEBMERCATOR2", multiPolygons, polygons_crs, startDate, endDate);

        System.out.println("pyramid = " + pyramid);
        assertFalse(pyramid.apply(0)._2().isEmpty());
    }
}
