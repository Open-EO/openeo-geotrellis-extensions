package org.openeo.geotrellis.water_vapor;

import java.io.Serializable;

import org.openeo.geotrellis.icor.LookupTable;

/*
 * This is a thin function object interface to be passed into the block calculator  
 */
public interface WaterVaporCalculator extends Serializable{
  public double computePixel(
	LookupTable lut,
	double sza,
	double vza,
	double raa,
	double dem,
	double aot,
	double cwv,
	double r0,
	double r1,
	double ozone,
	double invalid_value
  );		  
}

