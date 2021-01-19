package org.openeo.geotrellis.water_vapor;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.BeforeClass;
import org.junit.Test;
import org.openeo.geotrellis.icor.CorrectionDescriptorSentinel2;
import org.openeo.geotrellis.icor.LookupTable;
import org.openeo.geotrellis.icor.LookupTableIO;

import geotrellis.raster.CellType$;
import geotrellis.raster.DoubleCells;
import geotrellis.raster.DoubleConstantTile;
import geotrellis.raster.Tile;

public class testWaterVaporCalculator {

	private static LookupTable lut;
	private static CorrectionDescriptorSentinel2 cdS2=new CorrectionDescriptorSentinel2();
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
		return value*Math.PI/(Math.cos(sza*Math.PI/180.)*cdS2.getIrradiance(band));
	}
	
	@BeforeClass
    public static void setup_fields() throws Exception {
		lut=LookupTableIO.readLUT("https://artifactory.vgt.vito.be/auxdata-public/lut/S2A_all.bin");
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
		wvc.prepare(lut, cdS2, "B09", "B8A", "B11");
		assertEquals(wvc.wvBand,9);
		assertEquals(wvc.r0Band,8);
		assertEquals(wvc.r1Band,11);
		assertArrayEquals(wvc.refWeights,new double[]{0.269641,0.032007},1.e-4);
		assertArrayEquals(wvc.wv,new double[]{1.,2.,3.,3.5},1.e-4);
	}

	@Test
	public void testComputePixelInRange() throws Exception {

    	wvc.prepare(lut, cdS2, "B09", "B8A", "B11");
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
		assertEquals(r,0.5393735910926492,1.e-6);

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

    	wvc.prepare(lut, cdS2, "B09", "B8A", "B11");
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

}
