package org.openeo.geotrellis.icor;

//import org.apache.spark.SparkContext;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.broadcast.Broadcast;
//import java.io.IOException;
//import java.util.concurrent.Callable;
//import java.util.concurrent.ExecutionException;

import java.time.ZonedDateTime;

public abstract class ICorCorrectionDescriptor extends CorrectionDescriptor {
	/**
	 * This is the base class for the ICOR-based atmospheric correction.
	 * correct converts TOA radiance to BOA reflectance -> preScale function should convert digital number to TOA radiance
	 * 
	 */
	
	public LookupTable bcLUT;
	
	public ICorCorrectionDescriptor() throws Exception {
		bcLUT=LookupTableIO.readLUT(ICorCorrectionDescriptor.this.getLookupTableURL());
	}
	
	// extra interface functions specific to ICOR
    public abstract String getLookupTableURL();
	public abstract double getIrradiance(int iband);
	public abstract double getCentralWavelength(int iband);

    /**
     * @param src:              Band in reflectance range(0.,1.)
     * @param sza:      		sun zenith angle in degrees
     * @param time:             Time in millis from epoch
     * @param bandToConvert     Bandnumber
     * @return                  Band in radiance
     * @throws Exception 
     */
    // this is highly sub-optimal many things can be calculated beforehand once for all pixels!
    public double reflToRad(double src, double sza, ZonedDateTime time, int bandToConvert) {

        // SZA to SZA in rad + apply scale factor
        double szaInRadCoverage = 2.*sza*Math.PI/360.;

        // cos of SZA
        double cosSzaCoverage = Math.cos(szaInRadCoverage);

        double solarIrradiance = getIrradiance(bandToConvert);

        double radiance = src* (cosSzaCoverage * solarIrradiance) / (Math.PI);
        return radiance;
    }
    
    
    /**
     * General correction function using the lookup table to convert TOA radiance to BOA reflectance
     */
    double correctRadiance(int band, double TOAradiance, double sza, double vza, double raa, double gnd, double aot, double cwv, double ozone, int waterMask) {
        final double bgRad = TOAradiance;
        final double[] params = bcLUT.getInterpolated(band, sza, vza, raa, gnd, aot, cwv, ozone);
        double corrected = (-1. * params[0] + params[1] * TOAradiance + params[2] * bgRad) / (params[3] + params[4] * bgRad);
        if (waterMask != 0/*==LAND*/) {
            if (waterMask == 1/*==FRESH_WATER*/) {
                corrected -= params[5];
            } else {
                corrected -= params[6];
            }
        }
        return corrected;
    }
}
