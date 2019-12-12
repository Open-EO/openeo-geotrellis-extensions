package org.openeo.geotrellis.integrationtests;

import be.vito.eodata.gwcgeotrellis.wmts.WMTSServer;
import com.google.common.io.ByteStreams;
import geotrellis.layer.SpaceTimeKey;
import geotrellis.layer.TileLayerMetadata;
import geotrellis.raster.MultibandTile;
import geotrellis.raster.render.ColorMap;
import geotrellis.raster.render.ColorMap$;
import geotrellis.raster.render.ColorRamps;
import geotrellis.spark.pyramid.Pyramid;
import geotrellis.vector.Extent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.Ignore;
import org.junit.Test;
import org.openeo.geotrellisaccumulo.PyramidFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.immutable.Seq;

import java.io.InputStream;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.TreeMap;

@Ignore
public class WMTSServletTest {


    @Test
    public void testCreateWMTSFromRDD() throws Exception {

        SparkConf conf = new SparkConf();
        conf.setAppName("IngestS2");
        conf.setMaster("local[4]");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        SparkContext sc =SparkContext.getOrCreate(conf);

        String layername = "CGS_SENTINEL2_RADIOMETRY_V102";
        WMTSServer wmtsServer = new WMTSServer();
        wmtsServer.start();
        try{
            System.out.println("wmtsServer.getPort() = " + wmtsServer.getPort());
            PyramidFactory accumuloTileLayerRepository = new PyramidFactory("hdp-accumulo-instance", "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181");

            Extent bbox = new Extent(3, 51, 3.5, 52);
            Pyramid<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> pyramid = accumuloTileLayerRepository.pyramid(layername, bbox,"EPSG:4326", Option.apply( ZonedDateTime.parse("2017-07-01T00:00:00Z")),Option.apply( ZonedDateTime.parse("2017-09-01T00:00:00Z")));

            wmtsServer.addPyramidLayer(layername + "_RDD",pyramid);
            System.out.println("wmtsServer.getPort() = " + wmtsServer.getPort());
            wmtsServer.join();

            URI capabilities = wmtsServer.getURI().resolve("service/wmts?SERVICE=WMTS&request=GetCapabilities");
            InputStream inputStream = capabilities.toURL().openStream();
            String capabilitiesDoc = new String(ByteStreams.toByteArray(inputStream));
            System.out.println("capabilitiesDoc = " + capabilitiesDoc);
        }finally {
            wmtsServer.stop();

        }

    }

    @Test
    public void testWMTSFromColorMap() throws Exception {
        Configuration config = new Configuration();
        config.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(config);



        SparkConf conf = new SparkConf();
        conf.setAppName("IngestS2");
        conf.setMaster("local[4]");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        SparkContext sc =SparkContext.getOrCreate(conf);
        UserGroupInformation.setConfiguration(config);
        String layername = "PROBAV_L3_S10_TOC_NDVI_333M";
        WMTSServer wmtsServer = new WMTSServer();
        wmtsServer.start();
        try{
            System.out.println("wmtsServer.getPort() = " + wmtsServer.getPort());
            PyramidFactory accumuloTileLayerRepository = new PyramidFactory("hdp-accumulo-instance", "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181");

            Extent bbox = new Extent(10.5, 46.5, 11.4, 46.9);
            //Extent bbox = new Extent(3, 51, 3.5, 52);
            Seq<Tuple2<Object, RDD<Tuple2<SpaceTimeKey, MultibandTile>>>> pyramid = accumuloTileLayerRepository.pyramid_seq(layername, bbox,"EPSG:4326", Option.apply( ZonedDateTime.parse("2017-07-01T00:00:00Z")),Option.apply( ZonedDateTime.parse("2017-09-01T00:00:00Z")));

            int[] objectArray = new int[250];
            for (int i = 0; i < 250; i++) {
                objectArray[i]= i;
            }

            Map<Integer, RDD<Tuple2<SpaceTimeKey, MultibandTile>>> pyramidLevels = new TreeMap<>();
            for (Tuple2<Object, RDD<Tuple2<SpaceTimeKey, MultibandTile>>> e : JavaConversions.seqAsJavaList(pyramid)) {
                pyramidLevels.put((Integer) e._1, e._2());
            }
            ColorMap cm = ColorMap$.MODULE$.apply(objectArray, ColorRamps.GreenToRedOrange());
            wmtsServer.addPyramidLayer(layername + "_RDD",pyramidLevels, cm);
            System.out.println("wmtsServer.getPort() = " + wmtsServer.getPort());
            wmtsServer.join();

            URI capabilities = wmtsServer.getURI().resolve("service/wmts?SERVICE=WMTS&request=GetCapabilities");
            InputStream inputStream = capabilities.toURL().openStream();
            String capabilitiesDoc = new String(ByteStreams.toByteArray(inputStream));
            System.out.println("capabilitiesDoc = " + capabilitiesDoc);
        }finally {
            wmtsServer.stop();

        }

    }

    @Test
    public void testRDDLookup() {
        SparkConf conf = new SparkConf();
        conf.setAppName("IngestS2");
        conf.setMaster("local[4]");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.hadoop.fs.permissions.umask-mode", "000");
        SparkContext sc = SparkContext.getOrCreate(conf);
        String layername = "CGS_SENTINEL2_RADIOMETRY_V102";
        PyramidFactory accumuloTileLayerRepository = new PyramidFactory("hdp-accumulo-instance", "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181");

        Extent bbox = new Extent(3, 51, 4.5, 52);
        Pyramid<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> pyramid = accumuloTileLayerRepository.pyramid(layername,bbox,"EPSG:4326",Option.apply( ZonedDateTime.parse("2017-07-01T00:00:00Z")),Option.apply( ZonedDateTime.parse("2017-09-01T00:00:00Z")));

        accumuloTileLayerRepository.lookup(0,pyramid);
        //RDD<Tuple2<SpaceTimeKey, MultibandTile>> rdd = pyramid.level(0);

        /*PartitionPruningRDD<Tuple2<SpaceTimeKey, MultibandTile>> filteredRDD = PartitionPruningRDD.create(rdd, v1 -> {
            return false;
        });
        Option<Partitioner> partitioner = rdd.partitioner();
        */



    }
}
