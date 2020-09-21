package org.openeo.geotrellis.icor;

/**
 *
 * @author Sven Jochems
 */

// Applies MODTRAN atmospheric correction based on preset values in a lookup table.
public class CorrectionDescriptor{

    // Loops over each pixel in the raster and calculates the atmospheric correction
    public double correct(
    	LookupTable lut,
		int lutBand,
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
                
    	// TODO: handle nodata
    	
    	// Get interpolated array from lookuptable
        double[] params = lut.getInterpolated(lutBand,sza,vza,raa,gnd,aot,cwv,ozone);

        // Apply atmoshperic correction on pixel based on an array of parameters from MODTRAN
        double radiance=src;
        double bgRad=src;
        double corrected = (-1. * params[0] + params[1] * radiance + params[2] * bgRad) / (params[3] + params[4] * bgRad);
        if ( waterMask != 0/*==LAND*/ ){ 
            if (waterMask == 1/*==FRESH_WATER*/) { corrected -= params[5]; }
            else                                 { corrected -= params[6]; }
        }
        return corrected;
    }            

}
