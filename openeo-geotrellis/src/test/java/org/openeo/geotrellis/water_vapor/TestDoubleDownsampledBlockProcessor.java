package org.openeo.geotrellis.water_vapor;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;
import org.openeo.geotrellis.icor.LookupTable;

import geotrellis.raster.ArrayMultibandTile;
import geotrellis.raster.DoubleRawArrayTile;
import geotrellis.raster.MultibandTile;
import geotrellis.raster.Tile;

public class TestDoubleDownsampledBlockProcessor {

    @Test
    public void testWaterVaporCalculatorBlockCalculator() {

    	double vals[]=new double[]{
    		 0, 1, 2, 3, 4,   5, 6, 7, 8, 9,
     		10,11,12,13,14,  15,16,17,18,19,
    		20,21,22,23,24,  25,26,27,28,29,
    		30,31,32,33,34,  35,36,37,38,39,
    		40,41,42,43,44,  45,46,47,48,49,

    		50,51,52,53,54,  55,56,57,58,59,
    		60,61,62,63,64,  65,66,67,68,69,
    		70,71,72,73,74,  75,76,77,78,79,
    		80,81,82,83,84,  85,86,87,88,89,
    		90,91,92,93,94,  95,96,97,98,99
    	}; 

    	// because 10x10 block and giving 2 for blocksize
    	// double block processor resamples to int(size/(2*blocksize)) -> floor(10/(2*2))=2
    	double ref[]=new double[]{
			Double.NaN,Double.NaN,Double.NaN,Double.NaN,Double.NaN,   Double.NaN,Double.NaN,Double.NaN,Double.NaN,Double.NaN,
			Double.NaN,Double.NaN,Double.NaN,Double.NaN,Double.NaN,   Double.NaN,Double.NaN,Double.NaN,Double.NaN,Double.NaN,
			Double.NaN,Double.NaN,Double.NaN,Double.NaN,Double.NaN,   Double.NaN,Double.NaN,Double.NaN,Double.NaN,Double.NaN,
			Double.NaN,Double.NaN,Double.NaN,Double.NaN,Double.NaN,   Double.NaN,Double.NaN,Double.NaN,Double.NaN,Double.NaN,
			Double.NaN,Double.NaN,Double.NaN,Double.NaN,Double.NaN,   Double.NaN,Double.NaN,Double.NaN,Double.NaN,Double.NaN,
			
			144.0,144.0,144.0,144.0,144.0,                            154.0,154.0,154.0,154.0,154.0,
			144.0,144.0,144.0,144.0,144.0,                            154.0,154.0,154.0,154.0,154.0,
			144.0,144.0,144.0,144.0,144.0,                            154.0,154.0,154.0,154.0,154.0,
			144.0,144.0,144.0,144.0,144.0,                            154.0,154.0,154.0,154.0,154.0,
			144.0,144.0,144.0,144.0,144.0,                            154.0,154.0,154.0,154.0,154.0
    	};

    	Tile t0=new DoubleRawArrayTile(vals,10,10).mutable();
    	Tile t1=new DoubleRawArrayTile(vals,10,10).mutable();
    	Tile t2=new DoubleRawArrayTile(vals,10,10).mutable();
    	Tile t3=new DoubleRawArrayTile(vals,10,10).mutable();
    	Tile t4=new DoubleRawArrayTile(vals,10,10).mutable();
    	Tile t5=new DoubleRawArrayTile(vals,10,10).mutable();
    	Tile t6=new DoubleRawArrayTile(vals,10,10).mutable();
    	MultibandTile mbt=new ArrayMultibandTile(new Tile[]{t0,t1,t2,t3,t4,t5,t6});
    	Tile smbt=new DoubleDownsampledBlockProcessor().computeDoubleBlocks(mbt,2,0.,0.,Double.NaN, null, new WaterVaporCalculator() {
			@Override
			public double computePixel(LookupTable lut, double sza, double vza, double raa, double dem, double aot, double cwv, double r0, double r1, double ozone, double invalid_value) {
				return sza+cwv>=100. ? sza+cwv : invalid_value;
			}
		});
    	double[] result=smbt.toArrayDouble();
    	for (double i: result) System.out.println(i);
    	assertArrayEquals( result, ref, 1.e-6);

    }

    
    @Test
    public void testWaterVaporCalculatorAverage() {

    	double vals[]=new double[]{
    		Double.NaN,Double.NaN,Double.NaN,   Double.NaN,Double.NaN,Double.NaN,   Double.NaN,Double.NaN,Double.NaN,   Double.NaN, 
    		Double.NaN,Double.NaN,Double.NaN,   Double.NaN,Double.NaN,Double.NaN,   Double.NaN,Double.NaN,Double.NaN,   Double.NaN, 
    		Double.NaN,Double.NaN,Double.NaN,   Double.NaN,Double.NaN,Double.NaN,   Double.NaN,Double.NaN,Double.NaN,   Double.NaN,

    		100.0, 100.0, 100.0,                106.0, 106.0, 106.0,                112.0, 112.0, 112.0,                118.0,
    		100.0, 100.0, 100.0,                106.0, 106.0, 106.0,                112.0, 112.0, 112.0,                118.0, 
    		100.0, 100.0, 100.0,                106.0, 106.0, 106.0,                112.0, 112.0, 112.0,                118.0, 

    		120.0, 120.0, 120.0,                126.0, 126.0, 126.0,                132.0, 132.0, 132.0,                138.0, 
    		120.0, 120.0, 120.0,                126.0, 126.0, 126.0,                132.0, 132.0, 132.0,                138.0, 
    		120.0, 120.0, 120.0,                126.0, 126.0, 126.0,                132.0, 132.0, 132.0,                138.0, 

    		180.0, 180.0, 180.0,                186.0, 186.0, 186.0,                192.0, 192.0, 192.0,                198.0 
    	};    	
    	
    	double ref[]=new double[]{
        		127.2, 127.2, 127.2,   127.2, 127.2, 127.2,   127.2, 127.2, 127.2,   127.2, 
        		127.2, 127.2, 127.2,   127.2, 127.2, 127.2,   127.2, 127.2, 127.2,   127.2, 
        		127.2, 127.2, 127.2,   127.2, 127.2, 127.2,   127.2, 127.2, 127.2,   127.2,

        		100.0, 100.0, 100.0,   106.0, 106.0, 106.0,   112.0, 112.0, 112.0,   118.0,
        		100.0, 100.0, 100.0,   106.0, 106.0, 106.0,   112.0, 112.0, 112.0,   118.0, 
        		100.0, 100.0, 100.0,   106.0, 106.0, 106.0,   112.0, 112.0, 112.0,   118.0, 

        		120.0, 120.0, 120.0,   126.0, 126.0, 126.0,   132.0, 132.0, 132.0,   138.0, 
        		120.0, 120.0, 120.0,   126.0, 126.0, 126.0,   132.0, 132.0, 132.0,   138.0, 
        		120.0, 120.0, 120.0,   126.0, 126.0, 126.0,   132.0, 132.0, 132.0,   138.0, 

        		180.0, 180.0, 180.0,   186.0, 186.0, 186.0,   192.0, 192.0, 192.0,   198.0 
        	};    	

    	Tile t=new DoubleRawArrayTile(vals,10,10).mutable();
    	Tile at=new DoubleDownsampledBlockProcessor().replaceNoDataWithAverage(t, Double.NaN);
    	double[] result=at.toArrayDouble();
    	assertArrayEquals( result, ref, 1.e-6);

    }
    
}
