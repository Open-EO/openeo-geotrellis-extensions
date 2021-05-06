package org.openeo.geotrellis.icor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaPairRDD$;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openeo.geotrellis.smac.SMACCorrection;

import geotrellis.layer.*;
import geotrellis.proj4.*;
import geotrellis.raster.*;
import geotrellis.raster.geotiff.GeoTiffRasterSource;
import geotrellis.raster.stitch.Stitcher.MultibandTileStitcher$;
import geotrellis.spark.ContextRDD;
import geotrellis.spark.testkit.TileLayerRDDBuilders$;
import geotrellis.vector.Extent;
import scala.Option;
import scala.Tuple2;


public class TestAtmosphericCorrectionProcess {

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
	
	///////////////////////////////////////
	// Support functions
	///////////////////////////////////////
	
    public static Map<Integer,Integer> differentialHistogram(Tile t0, Tile t1, int sumabove, int resolution){
    	Map<Integer,Integer> res=new HashMap<>();
    	assertEquals((Integer)t0.cols(),(Integer)t1.cols());
    	assertEquals((Integer)t0.rows(),(Integer)t1.rows());
    	for (int icol=0; icol<(Integer)t0.cols(); ++icol)
        	for (int irow=0; irow<(Integer)t0.rows(); ++irow) {
        		int d=Math.abs(t0.get(icol,irow)-t1.get(icol,irow));
        		res.merge((d/resolution)*resolution>sumabove?sumabove:(d/resolution)*resolution, 1, (a, b) -> a + b);
        	}
    	return res;
    }
    
    public static void printHistogram(String name, Map<Integer,Integer> values) {
    	System.out.println(name);
    	double sum=values.values().stream().reduce(0, Integer::sum).doubleValue();
    	values.entrySet().stream().sorted(new Comparator<Entry<Integer,Integer>>() {
			@Override
			public int compare(Entry<Integer,Integer> o1, Entry<Integer,Integer> o2) {
				return o1.getKey().compareTo(o2.getKey());
			}
		}).peek((e) -> System.out.printf("%03d: % 7d (%.2f%%)\n",
			e.getKey(),
			e.getValue(),
			e.getValue().doubleValue()/sum*100.
		)).count();
    }

    public static ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> acTilesToSpaceTimeDataCube(
    		Tile[] tiles,
    		String datetime,
    		CRS crs,
    		Extent extent,
    		int tilesize
        ) {
			int ncols=1;
			int nrows=1;
			int tilecols=((Integer) tiles[0].cols());
			int tilerows=((Integer) tiles[0].rows());
    		if (tilesize>0) {
    			ncols=(tilecols/tilesize)+(tilecols%tilesize==0?0:1);
    			nrows=(tilerows/tilesize)+(tilerows%tilesize==0?0:1);
    			tilecols=tilesize;
    			tilerows=tilesize;
    		}
    	
            ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpatialKey>> datacube = 
        		(ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpatialKey>>) 
        		TileLayerRDDBuilders$.MODULE$.createMultibandTileLayerRDD(
        				SparkContext.getOrCreate(), 
        				new ArrayMultibandTile(tiles), 
        				new TileLayout(ncols, nrows, tilecols, tilerows)
        		);
            final ZonedDateTime minDate = ZonedDateTime.parse(datetime);
            JavaPairRDD<SpaceTimeKey, MultibandTile> spacetimeDataCube = JavaPairRDD$.MODULE$.fromJavaRDD(datacube.toJavaRDD()).flatMapToPair(spatialKeyMultibandTileTuple2 -> {
                return Arrays.asList(
                        Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(minDate)), spatialKeyMultibandTileTuple2._2)
                ).iterator();
            });
            TileLayerMetadata<SpatialKey> m = datacube.metadata();
            Bounds<SpatialKey> bounds = m.bounds();

            SpaceTimeKey minKey = SpaceTimeKey.apply(bounds.get().minKey(), TemporalKey.apply(minDate));
            KeyBounds<SpaceTimeKey> updatedKeyBounds = new KeyBounds<>(minKey,minKey);
            TileLayerMetadata<SpaceTimeKey> metadata = new TileLayerMetadata<>(
            	m.cellType(), 
            	m.layout(), 
//            	extent==null?m.extent():extent,
//            	crs==null?m.crs():crs, 
            	m.extent(),
               	m.crs(), 
            	updatedKeyBounds
            );

            return (ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>>) new ContextRDD(spacetimeDataCube.rdd(), metadata);
        }

    public static MultibandTile acGetAndStitchTiles(
    		RDD<Tuple2<SpaceTimeKey, MultibandTile>> resultRDD,
    		Integer[] indices,
    		int stitchedcols,
    		int stitchedrows	
    ) {
	    JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
	    List<Tuple2<SpaceTimeKey, MultibandTile>> collected = result.collect();
		
	    int tilecols=((Integer) collected.get(0)._2.band(0).cols());
		int tilerows=((Integer) collected.get(0)._2.band(0).rows());
		if (indices==null) {
			indices=new Integer[collected.get(0)._2.bandCount()];
			for(int i=0; i<collected.get(0)._2.bandCount(); ++i) indices[i]=i;
		}
	    
		scala.collection.immutable.List pieces=scala.collection.immutable.Nil$.MODULE$;
	    for(Tuple2<SpaceTimeKey, MultibandTile> i: collected) {
	    	Tile[] t=new Tile[indices.length];
	    	for(int idx=0; idx<indices.length; ++idx) t[idx]=i._2.band(indices[idx]);
	    	pieces=pieces.$colon$colon(
	    		new Tuple2<MultibandTile,Tuple2<Object,Object>>(
	    			new ArrayMultibandTile(t), 
	    			new Tuple2<Object,Object>(i._1.col()*tilecols,i._1.row()*tilerows)
	    		)
	    	);
	    }
	    return MultibandTileStitcher$.MODULE$.stitch(pieces, stitchedcols, stitchedrows);
    }


	///////////////////////////////////////
	// ICOR/S2 testing
	///////////////////////////////////////

    final ArrayList<String> icorS2BandIds=new ArrayList<String>();
    {
    	icorS2BandIds.add(new String("TOC-B02_10M"));
    	icorS2BandIds.add(new String("TOC-B03_10M"));
    	icorS2BandIds.add(new String("B04"));
    	icorS2BandIds.add(new String("TOC-B08_10M"));
    	icorS2BandIds.add(new String("TOC-B8A_20M"));
    	icorS2BandIds.add(new String("TOC-B09_60M"));
    	icorS2BandIds.add(new String("TOC-B11_20M"));
    	icorS2BandIds.add(new String("sunAzimuthAngles"));
    	icorS2BandIds.add(new String("sunZenithAngles"));
    	icorS2BandIds.add(new String("viewAzimuthMean"));
    	icorS2BandIds.add(new String("viewZenithMean"));
    }
    final ArrayList<Object> icorS2Params=new ArrayList<Object>();
    {
    	icorS2Params.add(Double.NaN);
    	icorS2Params.add(Double.NaN);
    	icorS2Params.add(Double.NaN);
    	icorS2Params.add(Double.NaN);
    	icorS2Params.add(Double.NaN);
    	icorS2Params.add(Double.NaN);
    	icorS2Params.add(new Double(0.33));
    }

    @Test
    public void testICORWithSentinel2() throws URISyntaxException {
    	String atmocorrDir = Paths.get(TestAtmosphericCorrectionProcess.class.getResource("atmocorr").toURI()).toAbsolutePath().toString();

    	Tile[] inputtiles=new Tile[] {
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_B02.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_B03.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_B04.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_B08.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_B8A.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_B09.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_B11.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_sunAzimuthAngles.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_sunZenithAngles.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_viewAzimuthMean.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_viewZenithMean.tif").toString()).read().get().tile().band(0)
    	};
    	
    	Tile[] reftiles=new Tile[] {
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_S2_B02.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_S2_B03.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_S2_B04.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_S2_B08.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_S2_SZA.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_S2_VZA.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_S2_RAA.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_S2_DEM.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_S2_AOT.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_S2_CWV.tif").toString()).read().get().tile().band(0)
    	};

        Extent extent = new Extent(655360,5676040,660480,5686280);
        CRS crs=geotrellis.proj4.CRS$.MODULE$.fromEpsgCode(32631);
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = acTilesToSpaceTimeDataCube(inputtiles,"2019-04-11T10:50:29Z",crs,extent,256);
        TileLayerMetadata<SpaceTimeKey> m = datacube.metadata();
        TileLayerMetadata<SpaceTimeKey> updatedMetadata = m.copy(m.cellType(),new LayoutDefinition(extent,m.layout().tileLayout()), extent,crs,m.bounds());
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> inputCube = new ContextRDD<>(datacube.rdd(), updatedMetadata);

        long start=System.nanoTime();
        
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new AtmosphericCorrection().correct(
        	"ICOR",
        	JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),
        	inputCube,
            icorS2BandIds,
            icorS2Params,
            "DEM",
        	"SENTINEL2",
        	true
        );      

        long end=System.nanoTime();
        
	    JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
		
		MultibandTile resulttiles = acGetAndStitchTiles(resultRDD, new Integer[]{0,1,2,3,11,12,13,14,15,16}, (Integer)inputtiles[0].cols(), (Integer)inputtiles[0].rows());
       
        final int limit=10;
        for(int i=0; i<resulttiles.bandCount(); ++i) {
        	Map<Integer,Integer> histo=differentialHistogram(resulttiles.band(i), reftiles[i], limit,1);
            printHistogram("bnd "+Integer.toString(i)+" -------------------------", histo);
            double sum=histo.values().stream().reduce(0, Integer::sum).doubleValue();
            double outliers=histo.getOrDefault(limit,0);
            assertTrue(outliers/sum<=0.01);
        }
        
        // to generate new reference data or to exact match it
        //geotrellis.raster.io.geotiff.MultibandGeoTiff$.MODULE$.apply(
        //	resulttiles, 
        //	extent, 
        //	crs
        //).write("unittest_data_S2_ICOR.tif",false);
        
        System.out.println("DONE ICOR+SENTINEL2, time="+Double.toString((double)(end-start)*1.0e-9));

    }


	///////////////////////////////////////
	// SMAC/S2 testing
	///////////////////////////////////////
      
    @Test
    public void testSMAC() {
        InputStream resource = SMACCorrection.class.getResourceAsStream("../smac/Coef_S2A_CONT_B02.dat");
        SMACCorrection.Coeff coeff = new SMACCorrection.Coeff(resource);

        int theta_s=45; //solar zenith angle
        int phi_s=200;  //solar azimuth angle
        int theta_v=5;  //viewing zenith angle
        int phi_v=-160; //viewing azimuth
        double pressure = 1013;//SMACCorrection.PdeZ(1300);

        double AOT550=0.1 ;// AOT at 550 nm
        double UO3=0.3    ;// Ozone content (cm)  0.3 cm= 300 Dobson Units
        double UH2O=0.3     ;// Water vapour (g/cm2)

        //compute the atmospheric correction
        double r_surf = SMACCorrection.smac_inv(0.2, theta_s, theta_v,phi_s - phi_v,(float) pressure,(float) AOT550, (float)UO3, (float)UH2O, coeff);
        System.out.println("r_surf = " + r_surf);
        //use reference python version to generate ref value: http://tully.ups-tlse.fr/olivier/smac-python
        assertEquals(0.16214342470440238,r_surf,0.00001);

    }

    final ArrayList<String> smacS2BandIds=new ArrayList<String>();
    {
    	smacS2BandIds.add(new String(   "B02"   ));
    	smacS2BandIds.add(new String("blaB03"   ));
    	smacS2BandIds.add(new String(   "B04bla"));
    	smacS2BandIds.add(new String(   "B08"   ));
    	smacS2BandIds.add(new String("SAA"));
    	smacS2BandIds.add(new String("sza"));
    	smacS2BandIds.add(new String("vaa"));
    	smacS2BandIds.add(new String("vza"));
    }
    final ArrayList<Object> smacS2Params=new ArrayList<Object>();
    {
    	//sza,vza,raa,gnd,aot,cwv,ozone
    	smacS2Params.add(Double.NaN);
    	smacS2Params.add(Double.NaN);
    	smacS2Params.add(Double.NaN);
    	smacS2Params.add(Double.NaN);
    	smacS2Params.add(Double.NaN);
    	smacS2Params.add(Double.NaN);
    	smacS2Params.add(new Double(0.33));
    }
    
    
	@Test
	public void testSMACWithSentinel2() throws URISyntaxException {
		String atmocorrDir = Paths.get(TestAtmosphericCorrectionProcess.class.getResource("atmocorr").toURI()).toAbsolutePath().toString();
	
		Tile[] inputtiles=new Tile[] {
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_B02.tif").toString()).read().get().tile().band(0),
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_B03.tif").toString()).read().get().tile().band(0),
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_B04.tif").toString()).read().get().tile().band(0),
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_B08.tif").toString()).read().get().tile().band(0),
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_sunAzimuthAngles.tif").toString()).read().get().tile().band(0),
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_sunZenithAngles.tif").toString()).read().get().tile().band(0),
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_viewAzimuthMean.tif").toString()).read().get().tile().band(0),
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_S2_viewZenithMean.tif").toString()).read().get().tile().band(0)
		};
		
		Tile[] reftiles=new Tile[] {
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_smac_S2_B02.tif").toString()).read().get().tile().band(0),
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_smac_S2_B03.tif").toString()).read().get().tile().band(0),
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_smac_S2_B04.tif").toString()).read().get().tile().band(0),
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_smac_S2_B08.tif").toString()).read().get().tile().band(0),
		};

		Tile[] icortiles=new Tile[] {
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_S2_B02.tif").toString()).read().get().tile().band(0),
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_S2_B03.tif").toString()).read().get().tile().band(0),
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_S2_B04.tif").toString()).read().get().tile().band(0),
		    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_S2_B08.tif").toString()).read().get().tile().band(0),
		};

	    Extent extent = new Extent(655360,5676040,660480,5686280);
	    CRS crs=geotrellis.proj4.CRS$.MODULE$.fromEpsgCode(32631);
	    ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = acTilesToSpaceTimeDataCube(inputtiles,"2019-04-11T10:50:29Z",crs,extent,256);
	    TileLayerMetadata<SpaceTimeKey> m = datacube.metadata();
	    TileLayerMetadata<SpaceTimeKey> updatedMetadata = m.copy(m.cellType(),new LayoutDefinition(extent,m.layout().tileLayout()), extent,crs,m.bounds());
	    ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> inputCube = new ContextRDD<>(datacube.rdd(), updatedMetadata);

        long start=System.nanoTime();
	    
	    ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new AtmosphericCorrection().correct(
	    	"SMAC",
	    	JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),
	    	inputCube,
	        smacS2BandIds,
	        smacS2Params,
	        "DEM",
	    	"SENTINEL2",
	    	true
	    );      

        long end=System.nanoTime();
	    
	    JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
		
		MultibandTile resulttiles = acGetAndStitchTiles(resultRDD, new Integer[]{0,1,2,3}, (Integer)inputtiles[0].cols(), (Integer)inputtiles[0].rows());

		
	    int limit=25;
	    for(int i=0; i<resulttiles.bandCount(); ++i) {
	    	Map<Integer,Integer> histo=differentialHistogram(resulttiles.band(i), reftiles[i], limit,1);
	        printHistogram("bnd "+Integer.toString(i)+" -------------------------", histo);
	        double sum=histo.values().stream().reduce(0, Integer::sum).doubleValue();
	        double outliers=histo.getOrDefault(limit,0);
            assertTrue(outliers/sum<=0.01);
	    }

	    // also check smac and icor do not deviate too much
	    limit=100;
	    for(int i=0; i<resulttiles.bandCount(); ++i) {
	    	Map<Integer,Integer> histo=differentialHistogram(resulttiles.band(i), icortiles[i], limit,20);
	        printHistogram("bnd "+Integer.toString(i)+" -------------------------", histo);
	        double sum=histo.values().stream().reduce(0, Integer::sum).doubleValue();
	        double outliers=histo.getOrDefault(limit,0);
            assertTrue(outliers/sum<=0.05);
	    }
	    
	    // to generate new reference data or to exact match it
	    //geotrellis.raster.io.geotiff.MultibandGeoTiff$.MODULE$.apply(
	    //	resulttiles, 
	    //	extent, 
	    //	crs
	    //).write("unittest_data_S2_SMAC.tif",false);

        System.out.println("DONE SMAC+SENTINEL2, time="+Double.toString((double)(end-start)*1.0e-9));
	
	}


	///////////////////////////////////////
	// ICOR/Landsat8 testing
	///////////////////////////////////////
    
   
    final ArrayList<String> icorL8BandIds=new ArrayList<String>();
    {
    	icorL8BandIds.add(new String("B02"));
    	icorL8BandIds.add(new String("B03"));
    	icorL8BandIds.add(new String("B04"));
    	icorL8BandIds.add(new String("SAA"));
    	icorL8BandIds.add(new String("sza"));
    	icorL8BandIds.add(new String("vaA"));
    	icorL8BandIds.add(new String("vza"));
    }
    final ArrayList<Object> icorL8Params=new ArrayList<Object>();
    {
    	//sza,vza,raa,gnd,aot,cwv,ozone
    	icorL8Params.add(Double.NaN);
    	icorL8Params.add(Double.NaN);
    	icorL8Params.add(Double.NaN);
    	icorL8Params.add(Double.NaN);
    	icorL8Params.add(Double.NaN);
    	icorL8Params.add(Double.NaN);
    	icorL8Params.add(new Double(0.33));
    }

    @Test
    public void testICORWithLandsat8() throws URISyntaxException {
    	String atmocorrDir = Paths.get(TestAtmosphericCorrectionProcess.class.getResource("atmocorr").toURI()).toAbsolutePath().toString();

    	Tile[] inputtiles=new Tile[] {
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_L8_B02.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_L8_B03.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_L8_B04.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_L8_SAA.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_L8_SZA.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_L8_VAA.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_L8_VZA.tif").toString()).read().get().tile().band(0)
    	};
    	// this is a nasty fix that VAA band is 0., but can't change nodata for that band alone
    	// since there is no cloud masking involved testing atmo corr, this is ok for B02-B04
    	for (int i=0; i<inputtiles.length; ++i) {
    		inputtiles[i]=inputtiles[i].convert(inputtiles[0].cellType()).withNoData(Option.apply(65535.));
    	}
    	
    	Tile[] reftiles=new Tile[] {
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_L8_B02.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_L8_B03.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_L8_B04.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_L8_SZA.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_L8_VZA.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_L8_RAA.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_L8_DEM.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_L8_AOT.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_L8_CWV.tif").toString()).read().get().tile().band(0)
    	};

        Extent extent = new Extent(655360,5676040,660480,5686280);
        CRS crs=geotrellis.proj4.CRS$.MODULE$.fromEpsgCode(32631);
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = acTilesToSpaceTimeDataCube(inputtiles,"2020-06-13T10:50:29Z",crs,extent,256);
        TileLayerMetadata<SpaceTimeKey> m = datacube.metadata();
        TileLayerMetadata<SpaceTimeKey> updatedMetadata = m.copy(m.cellType(),new LayoutDefinition(extent,m.layout().tileLayout()), extent,crs,m.bounds());
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> inputCube = new ContextRDD<>(datacube.rdd(), updatedMetadata);
         
        long start=System.nanoTime();
        
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new AtmosphericCorrection().correct(
        	"ICOR",
        	JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),
        	inputCube,
            icorL8BandIds,
            icorL8Params,
            "DEM",
        	"LANDSAT8",
        	true
        );      

        long end=System.nanoTime();
        
	    JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
		
		MultibandTile resulttiles = acGetAndStitchTiles(resultRDD, new Integer[]{0,1,2,7,8,9,10,11,12}, (Integer)inputtiles[0].cols(), (Integer)inputtiles[0].rows());
       
        final int limit=10;
        for(int i=0; i<resulttiles.bandCount(); ++i) {
        	Map<Integer,Integer> histo=differentialHistogram(resulttiles.band(i), reftiles[i], limit,1);
            printHistogram("bnd "+Integer.toString(i)+" -------------------------", histo);
            double sum=histo.values().stream().reduce(0, Integer::sum).doubleValue();
            double outliers=histo.getOrDefault(limit,0);
            assertTrue(outliers/sum<=0.01);
        }
        
        // to generate new reference data or to exact match it
        //geotrellis.raster.io.geotiff.MultibandGeoTiff$.MODULE$.apply(
        //	resulttiles, 
        //	extent, 
        //	crs
        //).write("unittest_data_L8_ICOR.tif",false);

        System.out.println("DONE ICOR+LANDSAT8, time="+Double.toString((double)(end-start)*1.0e-9));

    }

	///////////////////////////////////////
	// SMAC/Landsat8 testing
	///////////////////////////////////////
      
    final ArrayList<String> smacL8BandIds=new ArrayList<String>();
    {
    	smacL8BandIds.add(new String("B02"));
    	smacL8BandIds.add(new String("B03"));
    	smacL8BandIds.add(new String("B04"));
    	smacL8BandIds.add(new String("SAA"));
    	smacL8BandIds.add(new String("sza"));
    	smacL8BandIds.add(new String("vaA"));
    	smacL8BandIds.add(new String("vza"));
    }
    final ArrayList<Object> smacL8Params=new ArrayList<Object>();
    {
    	//sza,vza,raa,gnd,aot,cwv,ozone
    	smacL8Params.add(Double.NaN);
    	smacL8Params.add(Double.NaN);
    	smacL8Params.add(Double.NaN);
    	smacL8Params.add(Double.NaN);
    	smacL8Params.add(Double.NaN);
    	smacL8Params.add(Double.NaN);
    	smacL8Params.add(new Double(0.33));
    }
    
	@Test
	public void testSMACWithLandsat8() throws URISyntaxException {
		String atmocorrDir = Paths.get(TestAtmosphericCorrectionProcess.class.getResource("atmocorr").toURI()).toAbsolutePath().toString();
	
    	Tile[] inputtiles=new Tile[] {
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_L8_B02.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_L8_B03.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_L8_B04.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_L8_SAA.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_L8_SZA.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_L8_VAA.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_L8_VZA.tif").toString()).read().get().tile().band(0)
    	};
    	// this is a nasty fix that VAA band is 0., but can't change nodata for that band alone
    	// since there is no cloud masking involved testing atmo corr, this is ok for B02-B04
    	for (int i=0; i<inputtiles.length; ++i) {
    		inputtiles[i]=inputtiles[i].convert(inputtiles[0].cellType()).withNoData(Option.apply(65535.));
    	}
    	
    	Tile[] reftiles=new Tile[] {
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_L8_B02.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_L8_B03.tif").toString()).read().get().tile().band(0),
	    	GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_icor_L8_B04.tif").toString()).read().get().tile().band(0),
    	};

	    Extent extent = new Extent(655360,5676040,660480,5686280);
	    CRS crs=geotrellis.proj4.CRS$.MODULE$.fromEpsgCode(32631);
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = acTilesToSpaceTimeDataCube(inputtiles,"2020-06-13T10:50:29Z",crs,extent,256);
	    TileLayerMetadata<SpaceTimeKey> m = datacube.metadata();
	    TileLayerMetadata<SpaceTimeKey> updatedMetadata = m.copy(m.cellType(),new LayoutDefinition(extent,m.layout().tileLayout()), extent,crs,m.bounds());
	    ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> inputCube = new ContextRDD<>(datacube.rdd(), updatedMetadata);

        long start=System.nanoTime();
	    
	    ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new AtmosphericCorrection().correct(
	    	"SMAC",
	    	JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),
	    	inputCube,
	        smacL8BandIds,
	        smacL8Params,
	        "DEM",
	    	"SENTINEL2",
	    	true
	    );      

        long end=System.nanoTime();
	    
	    JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
		
		MultibandTile resulttiles = acGetAndStitchTiles(resultRDD, new Integer[]{0,1,2}, (Integer)inputtiles[0].cols(), (Integer)inputtiles[0].rows());
		
	    // results are compared to icor results, how close they are
	    int limit=130;
	    for(int i=0; i<resulttiles.bandCount(); ++i) {
	    	Map<Integer,Integer> histo=differentialHistogram(resulttiles.band(i), reftiles[i], limit,10);
	        printHistogram("bnd "+Integer.toString(i)+" -------------------------", histo);
	        double sum=histo.values().stream().reduce(0, Integer::sum).doubleValue();
	        double outliers=histo.getOrDefault(limit,0);
            assertTrue(outliers/sum<=0.05);
	    }
	   
	    // to generate new reference data or to exact match it
	    //geotrellis.raster.io.geotiff.MultibandGeoTiff$.MODULE$.apply(
	    //	resulttiles, 
	    //	extent, 
	    //	crs
	    //).write("unittest_data_L8_SMAC.tif",false);


//        for(int ib=0; ib<inputtiles.length; ++ib) {
//        	Tile it=inputtiles[ib];
//        	double a=0.;
//        	for(int i=0; i<(Integer)it.cols(); ++i)
//            	for(int j=0; j<(Integer)it.rows(); ++j)
//            		a+=it.getDouble(i, j);
//        	a/=(double)((Integer)it.cols()*(Integer)it.rows());
//        	System.out.println("Input band "+smacL8BandIds.get(ib)+": "+Double.toString(a));
//        }
//		resulttiles = acGetAndStitchTiles(resultRDD, new Integer[]{0,1,2,7,8,9,10,11,12}, (Integer)inputtiles[0].cols(), (Integer)inputtiles[0].rows());
//	    for(int ib=0; ib<resulttiles.bandCount(); ++ib) {
//        	Tile it=resulttiles.band(ib);
//        	double a=0.;
//        	for(int i=0; i<(Integer)it.cols(); ++i)
//            	for(int j=0; j<(Integer)it.rows(); ++j)
//            		a+=it.getDouble(i, j);
//        	a/=(double)((Integer)it.cols()*(Integer)it.rows());
//        	System.out.println("Result band "+Integer.toString(ib)+": "+Double.toString(a));
//        }
	    
	    
        System.out.println("DONE SMAC+LANDSAT8, time="+Double.toString((double)(end-start)*1.0e-9));
	
	}

}
