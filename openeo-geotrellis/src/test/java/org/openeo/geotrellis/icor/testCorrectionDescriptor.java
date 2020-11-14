package org.openeo.geotrellis.icor;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

public class testCorrectionDescriptor {

	private static LookupTable lut;

	@BeforeClass
    public static void LUT() throws Exception {
		lut=LookupTableIO.readLUT("https://artifactory.vgt.vito.be/auxdata-public/lut/S2A_all.bin");
    }

	private static class CorrectionInput {

		int band;
		long value;
		long time =  1577836800000L;
		double sza = 29.0;
		double vza = 5.0;
		double raa = 130.0;
		double gnd = 17;
		double aot = 0.28;
		double cwv = 2.64;
		double ozone = 0.33;
		int watermask = 0;

		public CorrectionInput(int band, long value, double gnd,double aot) {

			this.band = band;
			this.value = value;//reflectance, between 0 and 1
			this.gnd = gnd;//elevation in meters
			this.aot = aot;
		}
	}

	private CorrectionInput[] inputs = {new CorrectionInput(2,342L, 320.0,0.0001*1348.0)};

	@Test
	public void testCorrectionDescriptorCorrect() {
		CorrectionDescriptorSentinel2 cd=new CorrectionDescriptorSentinel2();
		double tbc=1.;
		double cv=cd.correct(lut, 1, 1577836800000L, tbc, 29.0, 5.0, 130.0, 0.0, 0.28, 2.64, 0.33, 0);
		System.out.println(Double.toString(tbc)+" -> "+Double.toString(cv));
		assertArrayEquals(new double[]{cv}, new double[]{1.4536087548063417}, 1.e-6);
	}

	CorrectionDescriptorSentinel2 cd = new CorrectionDescriptorSentinel2();
	@Test
	public void testCorrectionDescriptor() {
		for (int i = 0; i < inputs.length; i++) {
			CorrectionInput input = inputs[i];
			double cv = cd.correct(lut, input.band, input.time, input.value/10000.0, input.sza, input.vza, input.raa, input.gnd, input.aot, input.cwv, input.ozone, input.watermask);
			System.out.println("cv = " + cv);
		}
	}

//	@Test
//	public void testCorrectionDescriptorCorrectProfile() throws IOException {
//		LookupTable lut2=LookupTableIO.readLUT("lut_s2a");
//		CorrectionDescriptorSentinel2 cd=new CorrectionDescriptorSentinel2();
//		for (double tbc: Arrays.asList(
//				0.,
//				0.0001,
//				0.001,
//				0.01,
//				0.1,
//				1.,
//				10.,
//				100.,
//				1000.,
//				10000.
//		)) {
//			double cv=cd.correct(lut2, 1, 1577836800000L, tbc, 29.0, 5.0, 130.0, 0.0, 0.28, 2.64, 0.33, 0);
//			System.out.println(Double.toString(tbc)+" -> "+Double.toString(cv));
//		}
//	}

}


