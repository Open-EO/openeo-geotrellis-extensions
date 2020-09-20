package org.openeo.geotrellis.icor;

import geotrellis.raster.Tile;
import scala.Serializable;

import static scala.compat.java8.JFunction.*;


@SuppressWarnings("serial")
public class AtmosphericCorrectionICOR implements Serializable{
	
//	public static int getCorr(Object i) {
//		//System.out.println("GETCORR: "+i.getClass().toString());
//		return 8;
//	}
	
	Tile correct(int b, Tile t) {
		System.out.println("CLASS: "+t.getClass().toString()+" at band: "+Integer.toString(b));
		Tile r=t.map(func(i -> 11));
//		Tile r=t.map(func(i -> AtmosphericCorrectionICOR.getCorr(i)));
//		Tile r=t.map(func(AtmosphericCorrectionICOR::getCorr));
		return r;
	}
}

