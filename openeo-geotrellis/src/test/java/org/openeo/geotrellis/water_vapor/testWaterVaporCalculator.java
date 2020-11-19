package org.openeo.geotrellis.water_vapor;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.BeforeClass;
import org.junit.Test;
import org.openeo.geotrellis.icor.CorrectionDescriptorSentinel2;
import org.openeo.geotrellis.icor.LookupTable;
import org.openeo.geotrellis.icor.LookupTableIO;

public class testWaterVaporCalculator {

	private static LookupTable lut;
	private static CorrectionDescriptorSentinel2 cdS2=new CorrectionDescriptorSentinel2();
	private static WaterVaporCalculator wvc=new WaterVaporCalculator();

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
		wvc.prepare(lut, cdS2, "B09", new String[]{"B8A","B11"});
		assertEquals(wvc.wvBand,9);
		assertEquals(wvc.refBands[0],8);
		assertEquals(wvc.refBands[1],11);
		assertArrayEquals(wvc.refWeights,new double[]{0.269641,0.032007},1.e-4);
		assertArrayEquals(wvc.wv,new double[]{1.,2.,3.,3.5},1.e-4);
	}

	@Test
	public void testComputePixel() throws Exception {
		wvc.prepare(lut, cdS2, "B09", new String[]{"B8A","B11"});
		double r=wvc.computePixel(lut, 
			43.5725342155,
			6.95880821756,
			116.584011516,
			0.0, 
			0.1, 
			42.557835,  // cwv
			112.916855, // r0
			11.206167,  // r1
			0.33
		);
		assertEquals(r,0.5393735910926492,1.e-6);
	}
	
}
