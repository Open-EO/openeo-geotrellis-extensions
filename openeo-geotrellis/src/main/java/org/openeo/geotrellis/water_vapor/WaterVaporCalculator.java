package org.openeo.geotrellis.water_vapor;

import org.openeo.geotrellis.icor.CorrectionDescriptorSentinel2;
import org.openeo.geotrellis.icor.LookupTable;

// the inputs should be top of the atmosphere radiances

public class WaterVaporCalculator {

	public void prepare(LookupTable lut, CorrectionDescriptorSentinel2 cdS2, String wv_Band, String ref_Bands[]) throws Exception {

		// get band ids
		wvBand=cdS2.getBandFromName(wv_Band);
		refBands[0]=cdS2.getBandFromName(ref_Bands[0]);
		refBands[1]=cdS2.getBandFromName(ref_Bands[1]);

		// weights of the reference bands
	    refWeights[0]=(cdS2.getCentralWavelength(refBands[1]) - cdS2.getCentralWavelength(wvBand))      / (cdS2.getCentralWavelength(refBands[0]) + cdS2.getCentralWavelength(refBands[1]));
	    refWeights[1]=(cdS2.getCentralWavelength(wvBand)      - cdS2.getCentralWavelength(refBands[0])) / (cdS2.getCentralWavelength(refBands[0]) + cdS2.getCentralWavelength(refBands[1]));
	    
	    // initialize helper buffers
	    // to avoid always reallocating in a tight loop
	    wv=lut.getCwv();
	    wvluparams=new double[wv.length];
	    r0luparams=new double[wv.length];
	    r1luparams=new double[wv.length];
	    abda=new double[wv.length];
	 
	}
	
	public double computePixel(LookupTable lut,
		double sza,  // sun zenith angle [deg]
		double vza,  // sensor zenith angle [deg]
		double raa,  // relative azimuth angle [deg]
		double dem,  // elevation [???]
		double aot,  // aerosol optical thickness [???]
		double cwv,  // wv band input [???]
		double r0,   // reference band 0 input [???]
		double r1,   // reference band 1 input [???]
		double ozone // ozone [???]
	) {
		double v0=2.0; // starting value
		final int maxiter=100;
		final double eps=1.e-5;
/*
        for (size_t wv = 0; wv < cwv.size(); wv++) {
            params = this->lut->get( sza_val, vza_val, raa_val, elevation_val, aot_val, cwv[wv], ozone_val );
            float meas = watcor.get_toa_radiance(0.4, 0, params[this->measurement_band]);
            float ref_1 = watcor.get_toa_radiance(0.4, 0, params[this->reference_bands[0]]);
            float ref_2 = watcor.get_toa_radiance(0.4, 0, params[this->reference_bands[1]]);
            abda_model[wv] = (meas - params[measurement_band][0]);
            abda_model[wv] /= (this->reference_weights[0] * (ref_1 - params[reference_bands[0]][0]) + (this->reference_weights[1] * (ref_2 - params[reference_bands[1]][0])));
        }
*/		
		// get interpolated cwv/abda array
		for (int iwv=0; iwv<wv.length; ++iwv) {
			double wvParams[]=lut.getInterpolated(wvBand,      sza, vza, raa, dem, aot, wv[iwv], ozone);
			double r0Params[]=lut.getInterpolated(refBands[0], sza, vza, raa, dem, aot, wv[iwv], ozone);
			double r1Params[]=lut.getInterpolated(refBands[1], sza, vza, raa, dem, aot, wv[iwv], ozone);
            abda[iwv] = (cwv - wvParams[0]);
            abda[iwv] /= (refWeights[0] * (r0 - r0Params[0]) + (refWeights[1] * (r1 - r1Params[0])));
            wvluparams[iwv]=wvParams[0];
            r0luparams[iwv]=r0Params[0];
            r1luparams[iwv]=r1Params[0];
		}
/*
        while (abs(previous_estimate - watervapor) > 0.00001 && watervapor != invalid_value && iteration < max_pixel_iterations)
        {
            previous_estimate = watervapor;
            params = this->lut->get( sza_val, vza_val, raa_val, elevation_val, aot_val, watervapor, ozone_val );
            abda_eval = (measurement_radiance - params[this->measurement_band][0]);
            abda_eval /= (this->reference_weights[0] * (reference1_radiance - params[this->reference_bands[0]][0]) + this->reference_weights[1] * (reference2_radiance - params[this->reference_bands[1]][0]));
            watervapor = math::get_y_from_x_values<double>(abda_model, cwv, abda_eval);
            iteration++;
        }
*/
		int iiter=0;
		for(; iiter<=maxiter; ++iiter) {
			final double v1=v0;
			final double wvParam=interpolate(v0, wv, wvluparams);
			final double r0Param=interpolate(v0, wv, r0luparams);
			final double r1Param=interpolate(v0, wv, r1luparams);
            final double abda_eval = (cwv - wvParam) / (refWeights[0] * (r0 - r0Param) + refWeights[1] * (r1 - r1Param));
        	v0=interpolate(abda_eval, abda, wv);
        	if (Math.abs(v1-v0)<eps) break;
		}
		
/*        
		if (watervapor != invalid_value && watervapor >= 0.0 && watervapor <= cwv_max && watervapor>= cwv_min) {
            watervaporlist.push_back(watervapor);
        }
*/
		
		return 0.;
	}

	double interpolate(double xi, double[] x, double[] y) {
		int idx1=1;
		boolean di;
		boolean dn=(x[0]-xi)<0.;
		// searching for change of sign
		for (; idx1<x.length; ++idx1) {
			di=(x[idx1]-xi)<0.;
			if (di!=dn) break;
		}
		// if there was no change of sign, use the end closer to xi -> 
		// -> out of x range will result in extrapolation based on closest line segment
		if (idx1==x.length) idx1= xi<=x[0] ? 1 : x.length-1;
		// interpolate
		return y[idx1-1]+(xi-x[idx1-1])*(y[idx1]-y[idx1-1])/(x[idx1]-x[idx1-1]);
	}
	
	// bad indices
	int wvBand;
	int refBands[]= {-1,-1};
	
	// weights of the refeence bands
	double refWeights[]= {0.,0.};

	// crop limits
    double cwv_min=0.;
    double cwv_max=7.;

    double wv[];   // cwv - interolated:  cwv values
    double wvluparams[];   // lut[icwv][0] entries for all cwv-s in the lookup table for the water vapor band 
    double r0luparams[];   // lut[icwv][0] entries for all cwv-s in the lookup table for the 0th reference band
    double r1luparams[];   // lut[icwv][0] entries for all cwv-s in the lookup table for the 1st reference band
    double abda[]; // abda model
    
}

