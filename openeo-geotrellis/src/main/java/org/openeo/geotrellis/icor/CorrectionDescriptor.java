package org.openeo.geotrellis.icor;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;


/**
 *
 * @author Sven Jochems
 */

public abstract class CorrectionDescriptor implements Serializable{

	// parts to reimplement in specialization
	// -------------------------------------------
	
    public abstract int getBandFromName(String name) throws IllegalArgumentException;
    
    /**
     * This function converts the input digital number to usually top of the atmosphere (TOA) reflectance/radiance (see the documentation of the implementations)
     * If band is out of range, the function should return src (since any errors of mis-using bands should be caught upstream, before the pixel-wise loop).
     * @param src to be converted: this may be digital number, reflectance, radiance, ... depending on the specific correction, and it should clearly be documented in the implementation!
     * @param sza degree
     * @param time is usually needed for computing earth-sun distance
     * @param bandIdx band index as returned by getBandFromName
     * @return TOA reflectance/radiance as floating point (see the correct() function)
     */
    public abstract double preScale(double src, double sza, ZonedDateTime time, int bandIdx);


    /**
     * This function performs the pixel-wise correction: src is a pixel value belonging to band (as from getBandFromName).
     * If band is out of range, the function should return src (since any errors of mis-using bands should be caught upstream, before the pixel-wise loop).
     * @param bandName band id
     * @param bandIdx band index as returned by getBandFromName
     * @param src to be converted: the value from pre-scale, the implementation should document the value
     * @param sza degree
     * @param vza degree
     * @param raa degree
     * @param gnd km
     * @param aot
     * @param cwv 
     * @param ozone
     * @param waterMask
     * @return BOA reflectance * 10000 (i.e. in digital number)
     */
    // calculates the atmospheric correction for pixel
    public abstract double correct(
		String bandName,
		int bandIdx,
		ZonedDateTime time,
		double src, 
		double sza, 
		double vza, 
		double raa, 
		double gnd, 
		double aot, 
		double cwv, 
		double ozone,
		int waterMask
	);
	
	
	
	// common corrector code
	// -------------------------------------------


    /**
     * Get distance from earth to sun in Astronomical Units based on time in millis().
     * @param time millisec from epoch
     * @return earth-sun distance in AU
     */
    // This is not used anywhere currently, but might come handy later
    public static double earthSunDistance(ZonedDateTime time){
        
        // JD0 = number of days from 01/01/1950
        final ZonedDateTime D19500101 = LocalDate.of(1950, 1, 1).atStartOfDay(ZoneId.of("UTC"));
        final double JD0 = Duration.between(D19500101, time).toDays() ;
        
        final double T = JD0 - 10000.;        
        
        final double D    = Math.toRadians((11.786 + 12.190749 * T) % 360);
        final double XLP  = Math.toRadians((134.003 + 0.9856 * T) % 360);
        
        // Distance earth-sun [UA]
        final double DUA  = 1 / (1 + (1672.2 * Math.cos(XLP) + 28 * Math.cos(2*XLP) - 0.35 * Math.cos(D)) * 1e-5);
        
        // Distance earth-sun [meter]
        //final double Dm   = 0.14959787e12 * DUA;
        
        return DUA;
    }

    /**
     * Computes reflectance to radiance correction without considering earth-sun distance (this is how Sentinel-2 L1C is provided).
     * @param src:              Band in reflectance range0...1
     * @param sza:      		sun zenith angle in degrees
     * @param time:             Time in millis from epoch
     * @param bandToConvert     Bandnumber
     * @return                  Band in radiance
     * @throws Exception 
     */
    // this is highly sub-optimal many things can be calculated beforehand once for all pixels!
    public static double reflToRad(double src, double sza, double solarIrradiance) {

        // SZA to SZA in rad + apply scale factor
        double szaInRadCoverage = 2.*sza*Math.PI/360.;

        // cos of SZA
        double cosSzaCoverage = Math.cos(szaInRadCoverage);

        double radiance = src* (cosSzaCoverage * solarIrradiance) / (Math.PI);
        return radiance;
    }

    /**
     * Computes reflectance to radiance correction with considering earth-sun distance.
     * @param src:              Band in reflectance range 0...1
     * @param sza:      		sun zenith angle in degrees
     * @param time:             Time in millis from epoch
     * @param bandToConvert     Bandnumber
     * @return                  Band in radiance
     * @throws Exception 
     */
    // this is highly sub-optimal many things can be calculated beforehand once for all pixels!
    public static double reflToRad_with_earthsundistance(double src, double sza, ZonedDateTime time, double solarIrradiance) {

        // SZA to SZA in rad + apply scale factor
        double szaInRadCoverage = 2.*sza*Math.PI/360.;

        // cos of SZA
        double cosSzaCoverage = Math.cos(szaInRadCoverage);

        // TODO: ask Sinergise for the Sentinelhub layer what do they do with L1C, because it differs from stock L8 level1 data
        double earthsunAU = earthSunDistance(time);
        
        double radiance = src* (cosSzaCoverage * solarIrradiance) / (Math.PI * earthsunAU * earthsunAU);
        return radiance;
    }
    
}

