package org.openeo.geotrellis.water_vapor;

import org.openeo.geotrellis.icor.ICorCorrectionDescriptor;
import org.openeo.geotrellis.icor.LookupTable;

// the inputs should be top of the atmosphere radiances

public class AbdaWaterVaporCalculator implements WaterVaporCalculator{

	public void prepare(ICorCorrectionDescriptor cd, String wv_Band, String ref0_Band, String ref1_Band) throws Exception {

		// get band ids
		wvBand=cd.getBandFromName(wv_Band);
		r0Band=cd.getBandFromName(ref0_Band);
		r1Band=cd.getBandFromName(ref1_Band);

		// weights of the reference bands
	    refWeights[0]=(cd.getCentralWavelength(r1Band) - cd.getCentralWavelength(wvBand)) / (cd.getCentralWavelength(r0Band) + cd.getCentralWavelength(r1Band));
	    refWeights[1]=(cd.getCentralWavelength(wvBand) - cd.getCentralWavelength(r0Band)) / (cd.getCentralWavelength(r0Band) + cd.getCentralWavelength(r1Band));
	    
	    // initialize helper buffers
	    // to avoid always reallocating in a tight loop
	    wv=cd.bcLUT.getCwv();
	    wvluparams=new double[wv.length];
	    r0luparams=new double[wv.length];
	    r1luparams=new double[wv.length];
	    abda=new double[wv.length];
	 
	}

	@Override
	public double computePixel(
		LookupTable lut,
		double sza,   // sun zenith angle [deg]
		double vza,   // sensor zenith angle [deg]
		double raa,   // relative azimuth angle [deg]
		double dem,   // elevation [???]
		double aot,   // aerosol optical thickness [???]
		double cwv,   // wv band input [???]
		double r0,    // reference band 0 input [???]
		double r1,    // reference band 1 input [???]
		double ozone, // ozone [???]
		double invalid_value
	) {
		
		if ((cwv<0.0)||(cwv==invalid_value)||(Double.isNaN(cwv))) return invalid_value; 
		if ((r0<0.0)||(r0==invalid_value)||(Double.isNaN(r0))) return invalid_value; 
		if ((r1<0.0)||(r1==invalid_value)||(Double.isNaN(r1))) return invalid_value; 

		double v0=intialValue;
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
			double wvParams[]=lut.getInterpolated(wvBand, sza, vza, raa, dem, aot, wv[iwv], ozone);
			double r0Params[]=lut.getInterpolated(r0Band, sza, vza, raa, dem, aot, wv[iwv], ozone);
			double r1Params[]=lut.getInterpolated(r1Band, sza, vza, raa, dem, aot, wv[iwv], ozone);
			double icwv= get_toa_radiance(0.4, 0, wvParams);
			double ir0=  get_toa_radiance(0.4, 0, r0Params);
			double ir1=  get_toa_radiance(0.4, 0, r1Params);
            abda[iwv] = (icwv - wvParams[0]);
            abda[iwv] /= (refWeights[0] * (ir0 - r0Params[0]) + (refWeights[1] * (ir1 - r1Params[0])));
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
		// TODO: this is an undamped iteration -> prone to oscillate (-100 +100 -100 +100 ...), check if convergence radius could be computed and/or adding dissipation would lead anywhere
		int iiter=0;
		for(; iiter<=maxiter; ++iiter) {
			final double v1=v0;
			final double wvParam=interpolate(v0, wv, wvluparams);
			final double r0Param=interpolate(v0, wv, r0luparams);
			final double r1Param=interpolate(v0, wv, r1luparams);
            final double abda_eval = (cwv - wvParam) / (refWeights[0] * (r0 - r0Param) + refWeights[1] * (r1 - r1Param));
        	v0=interpolate(abda_eval, abda, wv);
        	//System.out.println("ITER: "+Double.toString(v0));
        	if (Math.abs(v1-v0)<eps) break;
		}
		
/*        
		if (watervapor != invalid_value && watervapor >= 0.0 && watervapor <= cwv_max && watervapor>= cwv_min) {
            watervaporlist.push_back(watervapor);
        }
*/

		if ((v0<vmin)||(v0>vmax)) return invalid_value;

/*		
		String msg=      Double.toString(sza)
					+" "+Double.toString(vza)
					+" "+Double.toString(raa)
					+" "+Double.toString(dem)
					+" "+Double.toString(aot)
					+" "+Double.toString(cwv)
					+" "+Double.toString(r0)
					+" "+Double.toString(r1)
					+" "+Double.toString(ozone)
					+" "+Double.toString(invalid_value)
					+" -> "+Double.toString(v0);
		System.out.println(msg);
*/
		
		return v0;
	}

	
    double get_toa_radiance(double reflectance,int water_land, double[] params)
    {
        /*
        from lookup table
        c1 = -params[0]
        c2 = params[1]
        c3 = params[2]
        c4 = params[3]
        c5 = params[4]
        d1f = params[5]
        d1s = params[6]
         rad = ((refl+ d1)*(c4+c5*bg) -c1 -c3*bg ) / c2 ) ;
        */
        double refl = reflectance;
        if (!(water_land == 0)) { // no water
            if (water_land == 1) { // freshwater
                refl = reflectance + params[5];
            }
            else {
                refl = reflectance + params[6];
            }
        }
        double target = (refl * params[3] - (-1.0 * params[0])) / (params[1] + params[2] - params[4] * refl);
        return target;
    }
	
	
	// TODO: this could be done much faster if cwv would be set to equidistant points when building from calling getInterpolated
	double interpolate(double xi, double[] x, double[] y) {
		int idx1=1;
		boolean di;
		boolean dn=x[0]<xi;
		// searching for change of sign
		for (; idx1<x.length; ++idx1) {
			di=x[idx1]<xi;
			if (di!=dn) break;
		}
		// if there was no change of sign, use the end closer to xi -> 
		// -> out of x range will result in extrapolation based on closest line segment
		if (idx1==x.length) 
			idx1= Math.abs(x[0]-xi)<Math.abs(x[x.length-1]-xi) ? 1 : x.length-1;
		// interpolate
		return y[idx1-1]+(xi-x[idx1-1])*(y[idx1]-y[idx1-1])/(x[idx1]-x[idx1-1]);
	}

    /// FIELDS ///////////////////////////////////////////////////////////////////////////////////

	// constants
	final double intialValue=2.0;
	final int maxiter=100;
	final double eps=1.e-5;
	final double vmin=0.;
	final double vmax=7.;
	
	// band indices
	int wvBand= -1;
	int r0Band= -1;
	int r1Band= -1;
	
	// weights of the reference bands
	double refWeights[]= {0.,0.};

	// temporary buffers (avoid realloc at every pixel)
	double wv[];   // cwv - interolated:  cwv values
    double wvluparams[];   // lut[icwv][0] entries for all cwv-s in the lookup table for the water vapor band 
    double r0luparams[];   // lut[icwv][0] entries for all cwv-s in the lookup table for the 0th reference band
    double r1luparams[];   // lut[icwv][0] entries for all cwv-s in the lookup table for the 1st reference band
    double abda[]; // abda model
    
    /// GETTERS/SETTERS ///////////////////////////////////////////////////////////////////////////////////
 
	public double getVmin() {
		return vmin;
	}

	public double getVmax() {
		return vmax;
	}

    
}

