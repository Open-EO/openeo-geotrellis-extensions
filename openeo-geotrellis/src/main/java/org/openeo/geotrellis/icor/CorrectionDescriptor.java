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

// Applies MODTRAN atmospheric correction based on preset values in a lookup table.
public abstract class CorrectionDescriptor implements Serializable{

	// parts to reimplement in specialization
	// -------------------------------------------
	
    public abstract int getBandFromName(String name) throws IllegalArgumentException;


    /**
     * This function performs the pixel-wise correction: src is a pixel value belonging to band (as from getBandFromName).
     * If band is out of range, the function should return src (since any errors of mis-using bands should be caught upstream, before the pixel-wise loop).
     * @param bandName band id
     * @param bandIdx band index as returned by getBandFromName
     * @param src to be converted: this may be digital number, reflectance, radiance, ... depending on the specific correction, and it should clearly be documented there!
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
     * @param time millisec from epoch
     * @return earth-sun distance in AU
     */
    // Get distance from earth to sun in Astronomical Units based on time in millis()
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
    
    
}

