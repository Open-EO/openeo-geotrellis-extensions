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

    // source:
    // https://oceancolor.gsfc.nasa.gov/docs/rsr/f0.txt
    // http://www.ioccg.org/groups/Thuillier.pdf
    //source on MEP: /data/TERRASCOPE/morpho_v2/process_data/process_data_20191021/auxdata/Solar_Irradiance/
    // sola irradiance is in mW/m2/nm
    // TODO: central wavelengths differ for S2A & S2B -> https://en.wikipedia.org/wiki/Sentinel-2
    // TODO: do a proper interpolation to central bandwidth, now just taken the nearest integer wavelength on the average of S2A & S2B mission specs
    // TODO: propagate referring by band name instead of index
    // Solar irradiance and earth sun distance (U) are part of L1C metadata, and not constant:
    /**
     * From MTD_MSIL1C.xml, top level of L1C product zip
     * /data/MTDA/CGS_S2/CGS_S2_L1C/2020/06/14/S2B_MSIL1C_20200614T104629_N0209_R051_T31UDS_20200614T132040/S2B_MSIL1C_20200614T104629_N0209_R051_T31UDS_20200614T132040.zip
     * <U>0.969998121389827</U>
     *         <Solar_Irradiance_List>
     *           <SOLAR_IRRADIANCE bandId="0" unit="W/m²/µm"> 1874.3</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="1" unit="W/m²/µm"> 1959.75</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="2" unit="W/m²/µm"> 1824.93</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="3" unit="W/m²/µm"> 1512.79</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="4" unit="W/m²/µm"> 1425.78</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="5" unit="W/m²/µm"> 1291.13</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="6" unit="W/m²/µm"> 1175.57</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="7" unit="W/m²/µm"> 1041.28</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="8" unit="W/m²/µm"> 953.93 </SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="9" unit="W/m²/µm"> 817.58 </SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="10" unit="W/m²/µm">365.41 </SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="11" unit="W/m²/µm">247.08 </SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="12" unit="W/m²/µm">  87.75</SOLAR_IRRADIANCE>
     *         </Solar_Irradiance_List>
     *
     *         Sentinel-2A list:
     *         <Solar_Irradiance_List>
     *           <SOLAR_IRRADIANCE bandId="0" unit="W/m²/µm">1913.57</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="1" unit="W/m²/µm">1941.63</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="2" unit="W/m²/µm">1822.61</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="3" unit="W/m²/µm">1512.79</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="4" unit="W/m²/µm">1425.56</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="5" unit="W/m²/µm">1288.32</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="6" unit="W/m²/µm">1163.19</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="7" unit="W/m²/µm">1036.39</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="8" unit="W/m²/µm">955.19</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="9" unit="W/m²/µm">813.04</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="10" unit="W/m²/µm">367.15</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="11" unit="W/m²/µm">245.59</SOLAR_IRRADIANCE>
     *           <SOLAR_IRRADIANCE bandId="12" unit="W/m²/µm">85.25</SOLAR_IRRADIANCE>
     *         </Solar_Irradiance_List>
     */
    double[] irradiances = {
        1874.3f,
        1959.75f,
        1824.93f,
        1512.79f,
        1425.78f,
        1291.13f,
        1175.57f,
        1041.28f,
        953.93f,
        817.58f,
        365.41f,
        247.08f,
          87.75f
    };

    // factor used for substitution between reflectance and radiance
    private double factor (Long time, int band) {
        /**
         * Radiometry
         *  band0: red
         *  band1: nir
         *  band2: blue
         *  band3: swir
         **/

        double solarIrradiance = irradiances[band];
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
