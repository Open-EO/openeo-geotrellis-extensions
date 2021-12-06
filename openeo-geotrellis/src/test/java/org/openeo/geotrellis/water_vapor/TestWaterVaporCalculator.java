package org.openeo.geotrellis.water_vapor;

import org.junit.BeforeClass;
import org.junit.Test;
import org.openeo.geotrellis.icor.CorrectionDescriptor;
import org.openeo.geotrellis.icor.ICorCorrectionDescriptor;
import org.openeo.geotrellis.icor.LookupTable;
import org.openeo.geotrellis.icor.LookupTableIO;
import org.openeo.geotrellis.icor.Sentinel2Descriptor;

import geotrellis.layer.SpaceTimeKey;
import geotrellis.raster.ArrayMultibandTile;
import geotrellis.raster.CellType$;
import geotrellis.raster.MultibandTile;
import geotrellis.raster.Tile;
import geotrellis.raster.FloatCells;
import geotrellis.raster.FloatConstantTile;
import scala.Tuple2;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.Arrays;

public class TestWaterVaporCalculator {

	private static LookupTable lut;
	private static ICorCorrectionDescriptor cd;
	static { 
		try {
			cd=new Sentinel2Descriptor();
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	private static AbdaWaterVaporCalculator wvc=new AbdaWaterVaporCalculator();

	static final double sza=43.5725342155;
	static final double vza=6.95880821756;
	static final double raa=116.584011516;
	static final double dem=0.0;
	static final double aot=0.1;
	static final double cwv=42.557835;
	static final double r0=112.916855;
	static final double r1=11.206167;
	static final double ozone=0.33;
	static final double invalid_value=Double.NaN;
	
	static double rad2refl(double value, int band) {
		return value;//*Math.PI/(Math.cos(sza*Math.PI/180.)*cd.getIrradiance(band));
	}
	
	@BeforeClass
    public static void setup_fields() throws Exception {
		lut=LookupTableIO.readLUT(cd.getLookupTableURL());
    }

	@Test
	public void testInterpolator() {
		double x[]={1., 1.75, 2., 4.};
		double y[]={2., 6.,   7., 8.};
		double z[]={8., 5.,   4., 3.};

		// forward: regular in-domain
		double r=wvc.interpolate(1.8, x, y);
		assertEquals(r,6.2,1.e-3);

		// forward: outside on the left
		r=wvc.interpolate(0.25, x, y);
		assertEquals(r,-2.,1.e-3);

		// forward: outside on the right
		r=wvc.interpolate(5., x, y);
		assertEquals(r,8.5,1.e-3);

		// reverse: regular in-domain
		r=wvc.interpolate(4.8, z, x);
		assertEquals(r,1.8,1.e-3);

		// reverse: outside on the left
		r=wvc.interpolate(11., z, x);
		assertEquals(r,0.25,1.e-3);

		// reverse: outside on the right
		r=wvc.interpolate(2.5, z, x);
		assertEquals(r,5.,1.e-3);
	}

	@Test
	public void testPrepare() throws Exception {
		wvc.prepare(cd, "B09", "B8A", "B11");
		assertEquals(wvc.wvBand,9);
		assertEquals(wvc.r0Band,8);
		assertEquals(wvc.r1Band,11);
		assertArrayEquals(wvc.refWeights,new double[]{0.269641,0.032007},1.e-4);
		assertArrayEquals(wvc.wv,new double[]{1.,2.,3.,3.5},1.e-4);
	}

	@Test
	public void testComputePixelInRange() throws Exception {

    	wvc.prepare(cd, "B09", "B8A", "B11");
		double r=wvc.computePixel(lut, 
			sza,
			vza,
			raa,
			dem, 
			aot, 
			rad2refl(cwv, wvc.wvBand),
			rad2refl(r0, wvc.r0Band),
			rad2refl(r1, wvc.r1Band),
			ozone,
			invalid_value
		);
		assertEquals(r,0.43244719821880784,1.e-6);

		r=wvc.computePixel(lut, 
			sza,
			vza,
			raa,
			dem, 
			aot, 
			Double.NaN,
			rad2refl(r0, wvc.r0Band),
			rad2refl(r1, wvc.r1Band),
			ozone,
			invalid_value
		);
		assertEquals(r,Double.NaN,1.e-6);

		r=wvc.computePixel(lut, 
			sza,
			vza,
			raa,
			dem, 
			aot, 
			rad2refl(cwv, wvc.wvBand),
			Double.NaN,
			rad2refl(r1, wvc.r1Band),
			ozone,
			invalid_value
		);
		assertEquals(r,Double.NaN,1.e-6);

		r=wvc.computePixel(lut, 
			sza,
			vza,
			raa,
			dem, 
			aot, 
			rad2refl(cwv, wvc.wvBand),
			rad2refl(r0, wvc.r0Band),
			Double.NaN,
			ozone,
			invalid_value
		);
		assertEquals(r,Double.NaN,1.e-6);
}

	
	@Test
	public void testComputePixelOutOfRange() throws Exception {

    	wvc.prepare(cd, "B09", "B8A", "B11");
		double r=wvc.computePixel(lut, 
			sza,
			vza,
			raa,
			dem, 
			aot, 
			rad2refl(75., wvc.wvBand),
			rad2refl(112., wvc.r0Band),
			rad2refl(11., wvc.r1Band),
			ozone,
			invalid_value

		);
		assertEquals(r,Double.NaN,1.e-6);

		r=wvc.computePixel(lut, 
			sza,
			vza,
			raa,
			dem, 
			aot, 
			rad2refl(0.1, wvc.wvBand),
			rad2refl(5., wvc.r0Band),
			rad2refl(15., wvc.r1Band),
			ozone,
			invalid_value
		);
		assertEquals(r,Double.NaN,1.e-6);
	}

	
    @Test
    public void testCWVProvider() throws Exception {

    	final float sza=43.5725342155f;
    	final float vza=6.95880821756f;
    	final float saa=116.584011516f;
    	final float vaa=0.f;
    	final float raa=saa-vaa;
    	final float aot=0.1f;
    	final float ozone=0.33f;
    	final float dem=0.05f;

    	final float cwv= 42.557835f;
    	final float r0= 112.916855f;
    	final float r1=  11.206167f;

    	Tile wvTile = new FloatConstantTile(cwv,256,256,(FloatCells)CellType$.MODULE$.fromName("float32raw").withDefaultNoData()).mutable();
        Tile r0Tile = new FloatConstantTile(r0,256,256,(FloatCells)CellType$.MODULE$.fromName("float32raw").withDefaultNoData()).mutable();
        Tile r1Tile = new FloatConstantTile(r1,256,256,(FloatCells)CellType$.MODULE$.fromName("float32raw").withDefaultNoData()).mutable();
        Tile vzaTile = new FloatConstantTile(vza,256,256,(FloatCells)CellType$.MODULE$.fromName("float32raw").withDefaultNoData()).mutable();
        Tile szaTile = new FloatConstantTile(sza,256,256,(FloatCells)CellType$.MODULE$.fromName("float32raw").withDefaultNoData()).mutable();
        Tile raaTile = new FloatConstantTile(raa,256,256,(FloatCells)CellType$.MODULE$.fromName("float32raw").withDefaultNoData()).mutable();
        Tile demTile = new FloatConstantTile(dem,256,256,(FloatCells)CellType$.MODULE$.fromName("float32raw").withDefaultNoData()).mutable();    
        
    	ArrayMultibandTile mbt=new ArrayMultibandTile(new Tile[]{wvTile,r0Tile,r1Tile,szaTile});
    	SpaceTimeKey stk=new SpaceTimeKey(0, 0, 0); // water vapor is not actually using the spacetimekey
    	
    	CorrectionDescriptor cd=new Sentinel2Descriptor();
    	
    	Tile result=new CWVProvider().compute(
    		new Tuple2<SpaceTimeKey, MultibandTile>(stk,mbt), 
    		szaTile, vzaTile, raaTile, 
    		demTile, 
    		aot, 
    		ozone, 
    		Arrays.asList(new String[] {"B09","_B8A","B11","sunZenithAngles"}), 
    		cd
    	);
    	
    	double resultAt00=result.getDouble(0, 0);
      	System.out.println(Double.toString(resultAt00));
		assertEquals(resultAt00,0.4369889795780182,1.e-6);

		double resultAtNN=result.getDouble(255, 255);
		System.out.println(Double.toString(resultAtNN));
		assertEquals(resultAtNN,0.4369889795780182,1.e-6);
		
    }
	
}
