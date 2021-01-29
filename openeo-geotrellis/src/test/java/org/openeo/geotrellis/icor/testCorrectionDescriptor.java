package org.openeo.geotrellis.icor;

import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

// TODO: this only tests Sentinel-2 descriptor only, extend to landsat8
public class testCorrectionDescriptor {

	private static LookupTable lut;
	private static CorrectionDescriptor cd = new Sentinel2Descriptor();

	@BeforeClass
    public static void LUT() throws Exception {
		lut=LookupTableIO.readLUT(cd.getLookupTableURL());
    }

	private static class CorrectionInput {

		int band;
		double value;
		ZonedDateTime time =  ZonedDateTime.now();
		double sza = 29.0;
		double vza = 5.0;
		double raa = 130.0;
		double expectedRadiance=-1.0; // after reflectance to radiance
		double expectedBOA=-1.0; // corrected value to bottom of atmosphere
		double gnd = 17;
		double aot = 0.28;
		double cwv = 2.64;
		double ozone = 0.33;
		int watermask = 0;

		public CorrectionInput(int band, double value, double gnd,double aot) {

			this.band = band;
			this.value = value;//reflectance, between 0 and 1
			this.gnd = gnd;//elevation in meters
			this.aot = aot;
		}

		public CorrectionInput(int band, double value,double expectedRadiance, double expectedBOA, double gnd,double aot,double saa,double sza,double vaa,double vza,double cwv, String date) {

			this.band = band;
			this.value = value;//reflectance, between 0 and 1
			this.expectedRadiance = expectedRadiance;
			this.expectedBOA = expectedBOA;
			this.gnd = gnd;//elevation in meters
			this.aot = aot;
			this.raa = saa-vaa;
			this.sza = sza;
			this.vza = vza;
			this.time = DateTimeFormatter.ISO_DATE_TIME.parse(date,ZonedDateTime::from);
		}
	}

	private CorrectionInput[] inputs = {
			//new CorrectionInput(2,342L, 320.0,0.0001*1348.0),
			//expected earth sun= 1.01751709288327
			new CorrectionInput(1,1267.,41.708855,         598.94135283652894,0.001,0.0001*1029.0,163,   57.8566,   69,2,     0.83, "2017-03-07T10:50:00Z"),//expected icor:574 sen2cor:598 AOT icor:0.078
			new CorrectionInput(3, 509.,17.925613561979805,348.4200980752023, 0.001,        0.082,129.13,    43.,  -1.,11.57, 0.357,"2019-04-11T10:50:29Z")//expected icor:353 sen2cor:336
	};
//'sunAzimuthAngles','sunZenithAngles','viewAzimuthMean','viewZenithMean'
	
	@Test
	public void testCorrectFunction() {
		for (int i = 0; i < inputs.length; i++) {
			CorrectionInput input = inputs[i];
			double cv = cd.correct(lut, input.band, input.time, input.value, input.sza, input.vza, input.raa, input.gnd, input.aot, input.cwv, input.ozone, input.watermask);
			System.out.println("cv = " + cv);
			assertArrayEquals(new double[]{cv}, new double[]{input.expectedBOA}, 1.e-6);
		}
	}

	@Test
	public void testCorrectRadiance() {
		double cv=cd.correctRadiance(lut,3,17.925,43.,11.57,129.13,0.,0.082,0.357,0.33,0);
		System.out.println("CORR="+Double.toString(cv));
		assertArrayEquals(new double[]{cv}, new double[]{0.03434362557030921}, 1.e-6);
	}

	@Test
	public void testReflectanceToRadiance() {
	    double rad=cd.reflToRad(10., 60., null, 1);
		System.out.println("REFL 2 RAD: "+Double.toString(rad));
		assertArrayEquals(new double[]{rad}, new double[]{3090.2001293264057}, 1.e-6);
	}
	
	@Test
	public void testSunEarthDistance() {
		double distance_au=cd.earthSunDistance(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1577836800000L), ZoneId.systemDefault()));
		System.out.println("sun-earth distance: "+Double.toString(distance_au));
		assertArrayEquals(new double[]{distance_au}, new double[]{0.9833099149733568}, 1.e-6);
	}
	
	

}


