package org.openeo.geotrellis.icor;

import static org.junit.Assert.*;

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
		CorrectionDescriptor cd=new CorrectionDescriptor();
		double tbc=1.;
		double cv=cd.correct(lut, 2, tbc, 1., 1., 1., 1., 1., 1., 1., 0);
		System.out.println(Double.toString(tbc)+" -> "+Double.toString(cv));
		assertArrayEquals(new double[]{cv}, new double[]{-0.010988655954946249}, 1.e-6);
	}

}


