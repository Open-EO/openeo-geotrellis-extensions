package org.openeo.geotrellis.icor;

// Applies MODTRAN atmospheric correction based on preset values in a lookup table.
public class Landsat8Descriptor extends CorrectionDescriptor{

    @Override
    public int getBandFromName(String name) throws Exception {
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
			default: throw new IllegalArgumentException("Unsupported band provided");
		}
	}
	
    // source:
    // official: http://www.gisagmaps.com/landsat-8-sentinel-2-bands/
    // B08 (from Thuillier spectrum): https://bleutner.github.io/RStoolbox/r/2016/01/26/estimating-landsat-8-esun-values
    // TODO: B10 and B11 has no values. Should be excluded from correction or extrap from Thuillier?
    // TODO: propagate referring by band name instead of index
    // TODO: to be checked if L1C is already corrected to earth-sun distance or not
    static double[] irradiances = {
	    1857,00, // B01
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
    static double[] central_wavelengths = {
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

    @Override
	public double getIrradiance(int iband) {
		return irradiances[iband];
	}

    @Override
	public double getCentralWavelength(int iband) {
		return central_wavelengths[iband];
	}
    
}
