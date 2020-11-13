package org.openeo.geotrellis.icor;

import geotrellis.layer.FloatingLayoutScheme;
import geotrellis.layer.LayoutLevel;
import geotrellis.layer.SpaceTimeKey;
import geotrellis.raster.CellSize;
import geotrellis.raster.RasterSource;
import geotrellis.raster.Tile;
import geotrellis.raster.geotiff.GeoTiffRasterSource;
import geotrellis.vector.Extent;
import geotrellis.vector.reproject.Reproject;
import org.junit.Test;
import org.openeo.geotrellis.ProjectedPolygons;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

public class TestAOTProvider {


    @Test
    public void testCAMSAOTProvider() throws URISyntaxException {
        URL camsLocation = TestAOTProvider.class.getResource("cams/CAMS_NRT_aod550_20170126T210000Z.tif");
        Path camsDir = Paths.get(camsLocation.toURI()).getParent();
        AOTProvider provider = new AOTProvider(camsDir.toAbsolutePath().resolve("CAMS_NRT_aod550_%1$tY%1$tm%1$tdT210000Z.tif").toString());
        Extent extent = new Extent(0.0, -1000000.0000, 833970.0 + 100000.0, 9329000.0 + 100000.0);
        LayoutLevel level = new FloatingLayoutScheme(256,256).levelFor(extent,new CellSize(10.0,10.0));

        final ZonedDateTime date = ZonedDateTime.parse("2017-01-26T00:00:00Z");
        ProjectedPolygons projectedPolygons = ProjectedPolygons.fromExtent(extent, "EPSG:32631");

        //key around 3.168952160641983, 50.42980280619309
        SpaceTimeKey key = SpaceTimeKey.apply(200, 1500, date);

        Tile tile = provider.computeAOT(key, projectedPolygons.crs(), level.layout());
        RasterSource rasterSource = GeoTiffRasterSource.apply(Paths.get(camsLocation.toURI()).toString());


        Extent keyBounds = level.layout().mapTransform().apply(key.spatialKey());
        Extent extentInOriginalCRS = Reproject.apply(keyBounds, projectedPolygons.crs(), rasterSource.crs());
        System.out.println("extentInOriginalCRS = " + extentInOriginalCRS);
        System.out.println("tile = " + tile);
        assertEquals(256,tile.cols());
        assertEquals(256,tile.rows());
        assertEquals(617,tile.get(0,0));
        //System.out.println(NumericEncoder.encodeIntegrals(tile));

    }

    @Test
    public void testComputeOverpasstime(){
        assertEquals(ZonedDateTime.parse("2017-01-26T11:00:00Z"),AOTProvider.computeUTCOverPasstime(0, ZonedDateTime.parse("2017-01-26T00:00:00Z")));
        assertEquals(ZonedDateTime.parse("2017-01-26T00:00:00Z"),AOTProvider.computeUTCOverPasstime(180, ZonedDateTime.parse("2017-01-26T00:00:00Z")));
        assertEquals(ZonedDateTime.parse("2017-01-26T17:00:00Z"), AOTProvider.computeUTCOverPasstime(-90, ZonedDateTime.parse("2017-01-26T00:00:00Z")));
        assertEquals(ZonedDateTime.parse("2017-01-26T17:00:00Z"), AOTProvider.computeUTCOverPasstime(-95.32156, ZonedDateTime.parse("2017-01-26T00:00:00Z")));
    }

    @Test
    public void testClosestPathFinder(){

        AOTProvider.ClosestPathFinder finder = new AOTProvider.ClosestPathFinder(ZonedDateTime.parse("2017-01-26T17:00:00Z"));
        finder.accept(Paths.get("/bla/bla/CAMS_NRT_aod550_20170126T210000Z.tif"));
        finder.accept(Paths.get("/bla/bla/CAMS_NRT_aod550_20170126T240000Z.tif"));
        Path expected = Paths.get("/bla/bla/CAMS_NRT_aod550_20170126T180000Z.tif");
        finder.accept(expected);
        finder.accept(Paths.get("/bla/bla/CAMS_NRT_aod550_20170126T150000Z.tif"));
        assertEquals(expected,finder.closestPath());
    }
}
