package org.openeo.geotrellis.icor;

import org.junit.BeforeClass;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertArrayEquals;

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

		public CorrectionInput(int band, long value, double gnd,double aot,double saa,double sza,double vaa,double vza,double cwv, String date) {

			this.band = band;
			this.value = value;//reflectance, between 0 and 1
			this.gnd = gnd;//elevation in meters
			this.aot = aot;
			this.raa = saa-vaa;
			this.sza = sza;
			this.vza = vza;
			this.time = DateTimeFormatter.ISO_DATE_TIME.parse(date,ZonedDateTime::from).toEpochSecond()*1000;
		}
	}

	private CorrectionInput[] inputs = {
			new CorrectionInput(2,342L, 320.0,0.0001*1348.0),
			new CorrectionInput(1,1267, 1.0/1000.0,0.0001*1029.0,163,58,69,2, 0.83,"2017-03-07T10:50:00Z")};//expected icor:542 sen2cor:599
//'sunAzimuthAngles','sunZenithAngles','viewAzimuthMean','viewZenithMean'
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


