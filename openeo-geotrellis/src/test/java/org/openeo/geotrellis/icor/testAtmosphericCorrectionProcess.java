package org.openeo.geotrellis.icor;

import geotrellis.layer.*;
import geotrellis.raster.*;
import geotrellis.spark.ContextRDD;
import geotrellis.spark.testkit.TileLayerRDDBuilders$;
import geotrellis.vector.Extent;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaPairRDD$;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.net.URL;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class testAtmosphericCorrectionProcess {

    private final ArrayList<String> bandIds=new ArrayList<String>();
    {
        bandIds.add(new String("TOC-B02_10M"));
        bandIds.add(new String("TOC-B09_60M"));
        bandIds.add(new String("TOC-B8A_20M"));
        bandIds.add(new String("TOC-B11_20M"));
    }

    private final ArrayList<Object> params=new ArrayList<Object>();
    {

        params.add(new Double(0.));
        params.add(new Double(0.));
        params.add(new Double(0.));
        params.add(new Double(0.));
        params.add(new Double(0.));
        params.add(new Double(0.));
        params.add(new Double(0.));
    }

    @BeforeClass
    public static void sparkContext() {
        SparkConf conf = new SparkConf();
        conf.setAppName("OpenEOTest");
        conf.setMaster("local[1]");
        //conf.set("spark.driver.bindAddress", "127.0.0.1");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkContext.getOrCreate(conf);
    }

    @AfterClass
    public static void shutDownSparkContext() {
        SparkContext.getOrCreate().stop();
    }

    public static ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> acTilesToSpaceTimeDataCube(
        	Tile b2Tile, Tile wvTile, Tile r0Tile, Tile r1Tile
        ) {

            ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpatialKey>> datacube = 
        		(ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpatialKey>>) 
        		TileLayerRDDBuilders$.MODULE$.createMultibandTileLayerRDD(
        				SparkContext.getOrCreate(), 
        				new ArrayMultibandTile(new Tile[]{b2Tile,wvTile,r0Tile,r1Tile}), 
        				new TileLayout(1, 1, ((Integer) wvTile.cols()), ((Integer) wvTile.rows()))
        		);
            final ZonedDateTime minDate = ZonedDateTime.parse("2017-01-01T00:00:00Z");
            JavaPairRDD<SpaceTimeKey, MultibandTile> spacetimeDataCube = JavaPairRDD$.MODULE$.fromJavaRDD(datacube.toJavaRDD()).flatMapToPair(spatialKeyMultibandTileTuple2 -> {
                return Arrays.asList(
                        Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(minDate)), spatialKeyMultibandTileTuple2._2)
                ).iterator();
            });
            TileLayerMetadata<SpatialKey> m = datacube.metadata();
            Bounds<SpatialKey> bounds = m.bounds();

            SpaceTimeKey minKey = SpaceTimeKey.apply(bounds.get().minKey(), TemporalKey.apply(minDate));
            KeyBounds<SpaceTimeKey> updatedKeyBounds = new KeyBounds<>(minKey,minKey);
            TileLayerMetadata<SpaceTimeKey> metadata = new TileLayerMetadata<>(m.cellType(), m.layout(), m.extent(),m.crs(), updatedKeyBounds);

            return (ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>>) new ContextRDD(spacetimeDataCube.rdd(), metadata);
        }
 
    
    @Test
    public void testAtmosphericCorrection() {

        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> inputCube = createInputCube();
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new AtmosphericCorrection().correct(
        	JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),
                inputCube,
        	bandIds,
        	params,
        	"DEM",
        	"SENTINEL2",
        	false
        );      
        System.out.println(resultRDD.getClass().toString());

        JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
        assertFalse(result.isEmpty());
        Map<SpaceTimeKey, MultibandTile> tiles = result.collectAsMap();
        
    }


    private ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> createInputCube() {
        Tile tileB2 = new IntConstantTile(1000,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        Tile wvTile = new IntConstantTile(2000,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        Tile r0Tile = new IntConstantTile(5000,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        Tile r1Tile = new IntConstantTile( 500,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = acTilesToSpaceTimeDataCube(tileB2,wvTile,r0Tile,r1Tile);
        TileLayerMetadata<SpaceTimeKey> m = datacube.metadata();
        Extent newExtent = new Extent(3.5, 50, 4.0, 51);
        TileLayerMetadata<SpaceTimeKey> updatedMetadata = m.copy(m.cellType(),new LayoutDefinition(newExtent,m.layout().tileLayout()), newExtent,m.crs(),m.bounds());
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> inputCube = new ContextRDD<>(datacube.rdd(), updatedMetadata);
        return inputCube;
    }

    @Test
    public void testSMACCorrectionOnCube() {

        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> inputCube = createInputCube();
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new AtmosphericCorrection().correct(
                "smac",
                JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),
                inputCube,
                bandIds,
                params,
                "DEM",
                "SENTINEL2",
                false
        );
        System.out.println(resultRDD.getClass().toString());

        JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
        assertFalse(result.isEmpty());
        Map<SpaceTimeKey, MultibandTile> tiles = result.collectAsMap();

    }

    @Test
    public void testSMAC() {
        URL resource = SMACCorrection.class.getResource("../smac/Coef_S2A_CONT_B2.dat");
        SMACCorrection.Coeff coeff = new SMACCorrection.Coeff(resource.getPath());

        int theta_s=45; //solar zenith angle
        int phi_s=200;  //solar azimuth angle
        int theta_v=5;  //viewing zenith angle
        int phi_v=-160;    //viewing azimuth
        double pressure = 1013;//SMACCorrection.PdeZ(1300);

        double AOT550=0.1 ;// AOT at 550 nm
        double UO3=0.3    ;// Ozone content (cm)  0.3 cm= 300 Dobson Units
        double UH2O=0.3     ;// Water vapour (g/cm2)

        //compute the atmospheric correction
        double r_surf = SMACCorrection.smac_inv(0.2f, theta_s, theta_v,phi_s - phi_v,(float) pressure,(float) AOT550, (float)UO3, (float)UH2O, coeff);
        System.out.println("r_surf = " + r_surf);
        //use reference python version to generate ref value: http://tully.ups-tlse.fr/olivier/smac-python
        assertEquals(0.16214342470440238,r_surf,0.00001);

    }

//    @Test
//    public void testAtmosphericCorrection2() {
//
//        Tile tile0 = new IntConstantTile(256,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
//        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = org.openeo.geotrellis.TestOpenEOProcesses.tileToSpaceTimeDataCube(tile0);
//        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new AtmosphericCorrection().correct(JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),datacube,"lut_s2a");
//        System.out.println(resultRDD.getClass().toString());
//
//        JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
//        assertFalse(result.isEmpty());
//        Map<SpaceTimeKey, MultibandTile> tiles = result.collectAsMap();
//        
//    }

}
