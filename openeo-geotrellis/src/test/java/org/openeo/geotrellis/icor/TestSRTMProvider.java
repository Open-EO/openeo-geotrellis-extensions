package org.openeo.geotrellis.icor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.ZonedDateTime;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import org.junit.Test;
import org.openeo.geotrellis.ProjectedPolygons;

import geotrellis.layer.FloatingLayoutScheme;
import geotrellis.layer.LayoutLevel;
import geotrellis.layer.SpaceTimeKey;
import geotrellis.raster.CellSize;
import geotrellis.raster.Tile;
import geotrellis.raster.geotiff.GeoTiffRasterSource;
import geotrellis.vector.Extent;

public class TestSRTMProvider {

    @Test
    public void testFileFinder() throws URISyntaxException {
        String srtmDir = Paths.get(TestSRTMProvider.class.getResource("srtm").toURI()).toAbsolutePath().toString();
        final ZonedDateTime date = ZonedDateTime.parse("2020-01-01T00:00:00Z"); // unused
        Extent extent = new Extent(620000, 5740000, 650000, 5780000);
        SpaceTimeKey key = SpaceTimeKey.apply(200, 1500, date);
        ProjectedPolygons projectedPolygons = ProjectedPolygons.fromExtent(extent, "EPSG:32631");
        
        scala.collection.immutable.List<String> filelist=SRTMProvider.findFiles(srtmDir, extent, projectedPolygons.crs());
        //System.out.println(filelist);

        assertEquals(filelist.size(),4);
        Set<String> check = new HashSet<>(Arrays.asList(
	        "N51E004.SRTMGL1.hgt.zip/N51E004.hgt",
	        "N52E004.SRTMGL1.hgt.zip/N52E004.hgt",
	        "N51E005.SRTMGL1.hgt.zip/N51E005.hgt",
	        "N52E005.SRTMGL1.hgt.zip/N52E005.hgt"
        ));

        for(int i=0; i<filelist.size(); i++) {
        	assertTrue(check.contains(filelist.apply(i).substring(filelist.apply(i).lastIndexOf("SRTMGL1")-8)));
        }
    }    	


    @Test
    public void testLoad() throws URISyntaxException {
    	
    	// setup AOI and provider
        String srtmDir = Paths.get(TestSRTMProvider.class.getResource("srtm").toURI()).toAbsolutePath().toString();
        final ZonedDateTime date = ZonedDateTime.parse("2020-01-01T00:00:00Z"); // unused
        Extent extent = new Extent(620000, 5660000, 660000, 5680000);
        ProjectedPolygons projectedPolygons = ProjectedPolygons.fromExtent(extent, "EPSG:32631");
        LayoutLevel level = new FloatingLayoutScheme(128,128).levelFor(extent,new CellSize(20.0,20.0));
    	SRTMProvider provider=new SRTMProvider(srtmDir);
    	
    	// single source 
        SpaceTimeKey key = SpaceTimeKey.apply(0, 0, date);
    	Tile tile=provider.compute(key, projectedPolygons.crs(), level.layout());
    	Tile reftile=GeoTiffRasterSource.apply(Paths.get(srtmDir.toString(),"test_srtm_tile_0_0.tif").toString()).read().get().tile().band(0);
    	assertArrayEquals(tile.toArrayDouble(),reftile.toArrayDouble(),1.e-6);

    	// on the edge of two SRTM tiles 
        key = SpaceTimeKey.apply(7, 0, date);
    	tile=provider.compute(key, projectedPolygons.crs(), level.layout());
    	reftile=GeoTiffRasterSource.apply(Paths.get(srtmDir.toString(),"test_srtm_tile_7_0.tif").toString()).read().get().tile().band(0);
    	assertArrayEquals(tile.toArrayDouble(),reftile.toArrayDouble(),1.e-6);
    }

}
