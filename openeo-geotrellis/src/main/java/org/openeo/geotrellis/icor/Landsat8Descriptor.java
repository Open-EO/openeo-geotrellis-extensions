package org.openeo.geotrellis.icor;

import java.time.ZonedDateTime;

// Applies MODTRAN atmospheric correction based on preset values in a lookup table.
public class Landsat8Descriptor extends ICorCorrectionDescriptor{


	public Landsat8Descriptor() throws Exception {
		super();
	}

	public String getLookupTableURL() {
		return "L8_big_disort"; 
	}

    @Override
    public int getBandFromName(String name) throws IllegalArgumentException {
		switch(name.toUpperCase()) {
			case "B01":         return 0;
			case "B02":         return 1;
			case "B03":         return 2;
			case "B04":         return 3;
			case "B05":         return 4;
			case "B06":         return 5;
			case "B07":         return 6;
			case "B08":         return 7;
			case "B09":         return 8;
			case "B10":         return 9;
			case "B11":         return 10;
			default: throw new IllegalArgumentException("Unsupported band: "+name);
		}
	}
    
    // source:
    // official: http://www.gisagmaps.com/landsat-8-sentinel-2-bands/
    // B08 (from Thuillier spectrum): https://bleutner.github.io/RStoolbox/r/2016/01/26/estimating-landsat-8-esun-values
    // TODO: B10 and B11 has no values. Should be excluded from correction or extrap from Thuillier?
    // TODO: to be checked if L1C is already corrected to earth-sun distance or not
    final static double[] irradiances = {
	    1857.00, // B01
	    2067.00, // B02
	    1893.00, // B03
	    1603.00, // B04
	     972.60, // B05
	     245.00, // B06
	      79.72, // B07
	    1723.88, // B08
	     399.70, // B09
	 Double.NaN, // B10
	 Double.NaN  // B11
    };

    // source:
    // http://www.gisagmaps.com/landsat-8-sentinel-2-bands/
    // 
    final static double[] central_wavelengths = {
         442.96, // B01
         482.04, // B02
         561.41, // B03
         654.59, // B04
         864.67, // B05
        1608.86, // B06
        2200.73, // B07
         589.50, // B08
        1373.43, // B09
       10895.00, // B10
       12005.00  // B11
    };

    // TODO: digital number to radiance scaling slighlty varies product-to product and should be taken from metadata
    // TODO: I am suspicious that this variation is due to the earth-sun distance correction -> to be checked 
    final static double[] RADIANCE_ADD_BAND = {
       -62.86466,
       -64.10541,
       -58.69893,
       -49.71440,
       -30.16725,
        -7.60064,
        -2.47247,
       -56.00013,
       -12.39698
    };

    final static double[] RADIANCE_MULT_BAND = {
        1.2573E-02,
        1.2821E-02,
        1.1740E-02,
        9.9429E-03,
        6.0335E-03,
        1.5201E-03,
        4.9449E-04,
        1.1200E-02,
        2.4794E-03
    };
    
    @Override
	public double getIrradiance(int iband) {
		return irradiances[iband];
	}

    @Override
	public double getCentralWavelength(int iband) {
		return central_wavelengths[iband];
	}

	/**
     * @param src: digital number of the top of the atmosphere TOA radiance 
     * @return TOA reflectance  (float 0..1)
	 */
	@Override
	public double preScale(double src, double sza, ZonedDateTime time, int bandIdx) {
		// lut only has 8 bands instead of 9
		if (bandIdx>7) return src;
		return reflToRad_with_earthsundistance(src*0.0001, sza, time, bandIdx);
	}
    
	/**
     * @param src: TOA radiance
     * @return: BOA reflectance in digital number
	 */
	@Override
    public double correct(
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
    		int waterMask)
    {
		// lut only has 8 bands instead of 9
		if (bandIdx>7) return src;
/*
		final double TOAradiance=src*RADIANCE_MULT_BAND[band]+RADIANCE_ADD_BAND[band];
        final double corrected = correctRadiance( band, TOAradiance, sza, vza, raa, gnd, aot, cwv, ozone, waterMask);
		//final double corrected=TOAradiance;
        return corrected*10000.;
*/
        // Apply atmoshperic correction on pixel based on an array of parameters from MODTRAN
        final double corrected = correctRadiance( bandIdx, src, sza, vza, raa, gnd, aot, cwv, ozone, waterMask);
		//final double corrected=TOAradiance;
        return corrected*10000.;    
    }

    /**
     * @param src:              Band in reflectance range(0.,1.)
     * @param sza:      		sun zenith angle in degrees
     * @param time:             Time in millis from epoch
     * @param bandToConvert     Bandnumber
     * @return                  Band in radiance
     * @throws Exception 
     */
    // this is highly sub-optimal many things can be calculated beforehand once for all pixels!
	// TODO: remove refltorad from L8 when refactored
    public double reflToRad_with_earthsundistance(double src, double sza, ZonedDateTime time, int bandToConvert) {

        // SZA to SZA in rad + apply scale factor
        double szaInRadCoverage = 2.*sza*Math.PI/360.;

        // cos of SZA
        double cosSzaCoverage = Math.cos(szaInRadCoverage);

        double solarIrradiance = getIrradiance(bandToConvert);

        // TODO: ask Sinergise for the Sentinelhub layer what do they do with L1C, because it differs from stock L8 level1 data
        double earthsunAU = earthSunDistance(time);
        
        double radiance = src* (cosSzaCoverage * solarIrradiance) / (Math.PI * earthsunAU * earthsunAU);
        return radiance;
    }
	
	
}
