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
		lut=LookupTableIO.readLUT("test_lut");
    }

	@Test
	public void testCorrectionDescriptorCorrect() {
		CorrectionDescriptorSentinel2 cd=new CorrectionDescriptorSentinel2();
		double tbc=1.;
		double cv=cd.correct(lut, 1, 1577836800000L, tbc, 29.0, 5.0, 130.0, 0.0, 0.28, 2.64, 0.33, 0);
		System.out.println(Double.toString(tbc)+" -> "+Double.toString(cv));
		assertArrayEquals(new double[]{cv}, new double[]{1.4536087548063417}, 1.e-6);
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


