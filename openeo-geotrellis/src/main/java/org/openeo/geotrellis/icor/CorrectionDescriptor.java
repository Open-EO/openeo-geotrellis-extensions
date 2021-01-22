package org.openeo.geotrellis.icor;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;


/**
 *
 * @author Sven Jochems
 */

// Applies MODTRAN atmospheric correction based on preset values in a lookup table.
public abstract class CorrectionDescriptor{

	// parts to reimplement in specialization
	// -------------------------------------------
	
    public abstract int getBandFromName(String name) throws Exception;
	public abstract double getIrradiance(int iband);
	//public abstract double getIrradiance(String iband) throws Exception;
	public abstract double getCentralWavelength(int iband);
	//public abstract double getCentralWavelength(String iband) throws Exception;
	
	// common corrector code
	// -------------------------------------------
    
    public static final ZonedDateTime D19500101 = LocalDate.of(1950, 1, 1).atStartOfDay(ZoneId.of("UTC"));

    /**
     * @param lut lookuptables
     * @param band band id
     * @param src reflectance to be converted [0...1]
     * @param sza degree
     * @param vza degree
     * @param raa degree
     * @param gnd ?UNIT?
     * @param aot
     * @param cwv 
     * @param ozone
     * @param waterMask
     * @return
     */
    // calculates the atmospheric correction for pixel
    public double correct(
    	LookupTable lut,
		int band,
		ZonedDateTime time,
		double src, 
		double sza, 
		double vza, 
		double raa, 
		double gnd, 
		double aot, 
		double cwv, 
		double ozone,
		int waterMask)
    {
    	// Get interpolated array from lookuptable

        // Apply atmoshperic correction on pixel based on an array of parameters from MODTRAN
        double radiance=reflToRad(src, sza, time, band);
        //TODO there's a step missing here:
        double corrected = correctRadiance(lut, band, radiance, sza, vza, raa, gnd, aot, cwv, ozone, waterMask);

        return corrected;
    }

    double correctRadiance(LookupTable lut, int band, double radiance,  double sza, double vza, double raa, double gnd, double aot, double cwv, double ozone, int waterMask) {
        double bgRad= radiance;
        double[] params = lut.getInterpolated(band, sza, vza, raa, gnd, aot, cwv, ozone);
        double corrected = (-1. * params[0] + params[1] * radiance + params[2] * bgRad) / (params[3] + params[4] * bgRad);
        if ( waterMask != 0/*==LAND*/ ){
            if (waterMask == 1/*==FRESH_WATER*/) { corrected -= params[5]; }
            else                                 { corrected -= params[6]; }
        }
        return corrected;
    }


    /**
     * @param src:              Band in reflectance range(0.,1.)
     * @param szaCoverage:      SZA in degrees 
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
     * @param time millisec from epoch
     * @return earth-sun distance in AU
     */
    // Get distance from earth to sun in Astronomical Units based on time in millis()
    public double earthSunDistance(ZonedDateTime time){
        
        // JD0 = number of days from 01/01/1950

        double JD0 = Duration.between(D19500101, time).toDays() ;
        
        double T = JD0 - 10000.;        
        
        double D    = Math.toRadians((11.786 + 12.190749 * T) % 360);
        double XLP  = Math.toRadians((134.003 + 0.9856 * T) % 360);
        
        // Distance earth-sun [UA]
        double DUA  = 1 / (1 + (1672.2 * Math.cos(XLP) + 28 * Math.cos(2*XLP) - 0.35 * Math.cos(D)) * 1e-5);
        
        // Distance earth-sun [meter]
        double Dm   = 0.14959787e12 * DUA;
        
        return DUA;
    }
    
    
}
