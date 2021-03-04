package org.openeo.geotrellis.icor;

import geotrellis.layer.*;
import geotrellis.proj4.CRS;
import geotrellis.proj4.*;
import geotrellis.proj4.CRS.*;
import geotrellis.raster.*;
import geotrellis.raster.stitch.*;
import geotrellis.raster.geotiff.GeoTiffRasterSource;
import geotrellis.spark.ContextRDD;
import geotrellis.spark.testkit.TileLayerRDDBuilders$;
import geotrellis.vector.Extent;
import net.bytebuddy.build.HashCodeAndEqualsPlugin.Sorted;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaPairRDD$;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.proj4j.CRSFactory;

import scala.Tuple2;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class testAtmosphericCorrectionProcess {

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
	
    public static Map<Integer,Integer> differentialHistogram(Tile t0, Tile t1, int sumabove){
    	Map<Integer,Integer> res=new HashMap<>();
    	assertEquals(t0.cols(),t1.cols());
    	assertEquals(t0.rows(),t1.rows());
    	for (int icol=0; icol<(Integer)t0.cols(); ++icol)
        	for (int irow=0; irow<(Integer)t0.rows(); ++irow) {
        		int d=Math.abs(t0.get(icol,irow)-t1.get(icol,irow));
        		res.merge(d>sumabove?sumabove:d, 1, (a, b) -> a + b);
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

//    public static ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> acTilesToSpaceTimeDataCube(
//    		Tile[] tiles
//    ) {
//      return acTilesToSpaceTimeDataCube(tiles,"2017-01-01T00:00:00Z",-1);
//    }

    public static MultibandTile acGetAndStitchTiles(
    		RDD<Tuple2<SpaceTimeKey, MultibandTile>> resultRDD,
    		Integer[] indices
    		
    ) {
      return null;
    }
    
	///////////////////////////////////////
	// ICOR testing
	///////////////////////////////////////
	

    final ArrayList<String> icorAllBandIds=new ArrayList<String>();
    {
    	icorAllBandIds.add(new String("TOC-B02_10M"));
    	icorAllBandIds.add(new String("TOC-B03_10M"));
    	icorAllBandIds.add(new String("TOC-B04_10M"));
    	icorAllBandIds.add(new String("TOC-B08_10M"));
    	icorAllBandIds.add(new String("TOC-B8A_20M"));
    	icorAllBandIds.add(new String("TOC-B09_60M"));
    	icorAllBandIds.add(new String("TOC-B11_20M"));
    	icorAllBandIds.add(new String("sunAzimuthAngles"));
    	icorAllBandIds.add(new String("sunZenithAngles"));
    	icorAllBandIds.add(new String("viewAzimuthMean"));
    	icorAllBandIds.add(new String("viewZenithMean"));
    }
    final ArrayList<Object> icorOnlyOzoneParams=new ArrayList<Object>();
    {
    	icorOnlyOzoneParams.add(Double.NaN);
    	icorOnlyOzoneParams.add(Double.NaN);
    	icorOnlyOzoneParams.add(Double.NaN);
    	icorOnlyOzoneParams.add(Double.NaN);
    	icorOnlyOzoneParams.add(Double.NaN);
    	icorOnlyOzoneParams.add(Double.NaN);
    	icorOnlyOzoneParams.add(new Double(0.33));
    }

    @Test
    public void testICOROnCube() throws URISyntaxException {
    	String atmocorrDir = Paths.get(testAtmosphericCorrectionProcess.class.getResource("atmocorr").toURI()).toAbsolutePath().toString();

    	Tile b02inptile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_B02.tif").toString()).read().get().tile().band(0);
    	Tile b03inptile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_B03.tif").toString()).read().get().tile().band(0);
    	Tile b04inptile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_B04.tif").toString()).read().get().tile().band(0);
    	Tile b08inptile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_B08.tif").toString()).read().get().tile().band(0);
    	Tile b8ainptile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_B8A.tif").toString()).read().get().tile().band(0);
    	Tile b09inptile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_B09.tif").toString()).read().get().tile().band(0);
    	Tile b11inptile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_B11.tif").toString()).read().get().tile().band(0);
    	Tile saainptile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_sunAzimuthAngles.tif").toString()).read().get().tile().band(0);
    	Tile szainptile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_sunZenithAngles.tif").toString()).read().get().tile().band(0);
    	Tile vaainptile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_viewAzimuthMean.tif").toString()).read().get().tile().band(0);
    	Tile vzainptile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_input_viewZenithMean.tif").toString()).read().get().tile().band(0);
    	Tile[] inputtiles=new Tile[] {
        		b02inptile,
        		b03inptile,
        		b04inptile,
        		b08inptile,
        		b8ainptile,
        		b09inptile,
        		b11inptile,
        		saainptile,
        		szainptile,
        		vaainptile,
        		vzainptile
    	};
    	
    	Tile vzareftile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_VZA.tif").toString()).read().get().tile().band(0);
    	Tile szareftile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_SZA.tif").toString()).read().get().tile().band(0);
    	Tile raareftile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_RAA.tif").toString()).read().get().tile().band(0);
    	Tile demreftile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_DEM.tif").toString()).read().get().tile().band(0);
    	Tile cwvreftile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_CWV.tif").toString()).read().get().tile().band(0);
    	Tile aotreftile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_AOT.tif").toString()).read().get().tile().band(0);
    	Tile b02reftile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_B02.tif").toString()).read().get().tile().band(0);
    	Tile b03reftile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_B03.tif").toString()).read().get().tile().band(0);
    	Tile b04reftile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_B04.tif").toString()).read().get().tile().band(0);
    	Tile b08reftile=GeoTiffRasterSource.apply(Paths.get(atmocorrDir.toString(),"ref_check_B08.tif").toString()).read().get().tile().band(0);
    	Tile[] reftiles=new Tile[] {
    		b02reftile,
    		b03reftile,
    		b04reftile,
    		b08reftile,
    		szareftile,
    		vzareftile,
    		raareftile,
    		demreftile,
    		aotreftile,
    		cwvreftile
    	};

        Extent extent = new Extent(655360,5676040,660480,5686280);
        CRS crs=geotrellis.proj4.CRS$.MODULE$.fromEpsgCode(32631);
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = acTilesToSpaceTimeDataCube(inputtiles,"2019-04-11T10:50:29Z",crs,extent,256);
        TileLayerMetadata<SpaceTimeKey> m = datacube.metadata();
        TileLayerMetadata<SpaceTimeKey> updatedMetadata = m.copy(m.cellType(),new LayoutDefinition(extent,m.layout().tileLayout()), extent,crs,m.bounds());
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> inputCube = new ContextRDD<>(datacube.rdd(), updatedMetadata);
         
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new AtmosphericCorrection().correct(
        	JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),
        	inputCube,
            icorAllBandIds,
            icorOnlyOzoneParams,
            "DEM",
        	"SENTINEL2",
        	true
        );      
        System.out.println(resultRDD.getClass().toString());

	    JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
	    assertFalse(result.isEmpty());
	    List<Tuple2<SpaceTimeKey, MultibandTile>> collected = result.collect();
		
	    int tilecols=256;//((Integer) collected.get(0)._2.band(0).cols());
		int tilerows=256;//((Integer) collected.get(0)._2.band(0).rows());

		scala.collection.immutable.List pieces=scala.collection.immutable.Nil$.MODULE$;
	    for(Tuple2<SpaceTimeKey, MultibandTile> i: collected)
	    	pieces=pieces.$colon$colon(
	    		new Tuple2<MultibandTile,Tuple2<Object,Object>>(
	    			new ArrayMultibandTile(new Tile[] {
	    			    i._2.band(0),
	    			    i._2.band(1),
	    			    i._2.band(2),
	    			    i._2.band(3),
	    			    i._2.band(11),
	    			    i._2.band(12),
	    			    i._2.band(13),
	    			    i._2.band(14),
	    			    i._2.band(15),
	    			    i._2.band(16)
	    			}), 
	    			new Tuple2<Object,Object>(i._1.col()*tilecols,i._1.row()*tilerows)
	    		)
	    	);
	    MultibandTile tiles=(new Stitcher$MultibandTileStitcher$()).stitch(pieces, (Integer)b02inptile.cols(), (Integer)b02inptile.rows());
        
        final int limit=10;
        for(int i=0; i<10; ++i) {
        	Map<Integer,Integer> histo=differentialHistogram(tiles.band(i), reftiles[i], limit);
            printHistogram("bnd "+Integer.toString(i)+" -------------------------", histo);
            double sum=histo.values().stream().reduce(0, Integer::sum).doubleValue();
            double outliers=histo.getOrDefault(limit,0);
            assertTrue(outliers/sum<=0.01);
        }

        // to generate new reference data or to exact match it
        /*
        geotrellis.raster.io.geotiff.MultibandGeoTiff$.MODULE$.apply(
        	tiles, 
        	newExtent, 
        	crs
        ).write("unittest_data.tif",false);
        */
        System.out.println("DONE");

    }
    
	///////////////////////////////////////
	// SMAC testing
	///////////////////////////////////////
    

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
    
    
    private ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> createInputCube() {
        Tile tileB2 = new IntConstantTile(1000,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        Tile wvTile = new IntConstantTile(2000,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        Tile r0Tile = new IntConstantTile(5000,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        Tile r1Tile = new IntConstantTile( 500,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = acTilesToSpaceTimeDataCube(new Tile[] {tileB2,wvTile,r0Tile,r1Tile}, "2017-01-01T00:00:00Z", null, null, -1);
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

   
}
