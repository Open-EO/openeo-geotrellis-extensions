package org.openeo.geotrellis.icor;

import java.time.LocalDate;


/**
 *
 * @author Sven Jochems
 */

// Applies MODTRAN atmospheric correction based on preset values in a lookup table.
public class CorrectionDescriptorSentinel2{

	public int getBandFromName(String name) throws Exception {
		switch(name.toUpperCase()) {
			case "TOC-B02_10M": return 1; // blue
            case "B02": return 1; // blue
            case "TOC-B03_10M": return 2; // green
            case "B03": return 2; // green
			case "TOC-B04_10M": return 3; // red
            case "B04": return 3; // red
			case "TOC-B08_10M": return 7; // nir
            case "B08": return 7; // nir
			case "TOC-B11_20M": return 10; // swir
			default: throw new Exception("Unsupported band provided");
		}
	}
	
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
    // Loops over each pixel in the raster and calculates the atmospheric correction
    public double correct(
    	LookupTable lut,
		int band,
		long time,
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
        double[] params = lut.getInterpolated(band,sza,vza,raa,gnd,aot,cwv,ozone);
        
        // Apply atmoshperic correction on pixel based on an array of parameters from MODTRAN
        double radiance=reflToRad(src, sza, time, band);
        double bgRad=radiance;
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
    public double reflToRad(double src, double sza, long time, int bandToConvert) {

        // SZA to SZA in rad + apply scale factor
    	// note sza is scaling by 2. is moved inside
        //GridCoverage2D szaInRadCoverage = multiplyConst(szaCoverage, new double[]{Math.PI/360}, -1.);
        double szaInRadCoverage = 2.*sza*Math.PI/360.;

        // cos of SZA
        //GridCoverage2D cosSzaCoverage = cosine(szaInRadCoverage, -1.);
        double cosSzaCoverage = Math.cos(szaInRadCoverage);
        
        // multiply with reflectance
       	// note band scaling by 2000. is moved inside
        //GridCoverage2D multiplyCoverage = multiply(bandCoverage, cosSzaCoverage, -1.);
        double multiplyCoverage = 2000.*src*cosSzaCoverage;
        
        // divide by factor and scale
        //GridCoverage2D radiance = divideByConst(multiplyCoverage, new double[]{factor(time, bandToConvert)*2000}, -1.);        
        double radiance = multiplyCoverage/(factor(time, bandToConvert)*2000.);
        
        return radiance;
    }

    
    // factor used for substitution between reflectance and radiance
    private double factor (Long time, int band) {
        double solarIrradiance=-1.;
        
        /**
         * Radiometry
         *  band0: red
         *  band1: nir
         *  band2: blue
         *  band3: swir
         **/
        
        // source:
        // https://oceancolor.gsfc.nasa.gov/docs/rsr/f0.txt
        // http://www.ioccg.org/groups/Thuillier.pdf
        // sola irradiance is in mW/m2/nm
        // TODO: central wavelengths differ for S2A & S2B -> https://en.wikipedia.org/wiki/Sentinel-2
        // TODO: do a proper interpolation to central bandwidth, now just taken the nearest integer wavelength on the average of S2A & S2B mission specs
        // TODO: propagate referring by band name instead of index
        
        switch (band){
            case 1: solarIrradiance = 2021.260010; //blue B02
                break;
            case 2: solarIrradiance = 1921.941; //green B03
                break;
            case 7: solarIrradiance = 1039.760010; //nir B08
                break;
            case 3: solarIrradiance = 1553.199951; //red B04
                break;
            case 10: solarIrradiance = 250.481003; //swir B11
                break;
/*
            case 'B01': solarIrradiance = 1953.449; break; //442nm
            case 'B02': solarIrradiance = 1921.941; break; //492nm
            case 'B03': solarIrradiance = 1744.613; break; //559nm
            case 'B04': solarIrradiance = 1535.771; break; //665nm
            case 'B05': solarIrradiance = 1438.637; break; //704nm
            case 'B06': solarIrradiance = 1273.076; break; //740nm
            case 'B07': solarIrradiance = 1163.672; break; //781nm
            case 'B08': solarIrradiance = 1025.549; break; //833nm
            case 'B8A': solarIrradiance =  994.924; break; //864nm
            case 'B09': solarIrradiance =  804.205; break; //944nm
            case 'B10': solarIrradiance =  364.619; break; //1375nm
            case 'B11': solarIrradiance =  244.695; break; //1612nm
            case 'B12': solarIrradiance =   86.909; break; //2194nm
*/
        }
        return Math.PI * earthSunDistance(time) * earthSunDistance(time) / solarIrradiance;
    }
    
    /**
     * @param time millisec from epoch
     * @return earth-sun distance in AU
     */
    // Get distance from earth to sun in Astronomical Units based on time in millis()
    public double earthSunDistance(long time){
        
        // JD0 = number of days from 01/01/1950
        double JD0 = ((double)time)/86.4e6 - ((double)LocalDate.of(1950,1,1).toEpochDay()); 
        
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
