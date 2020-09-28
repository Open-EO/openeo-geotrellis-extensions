package org.openeo.geotrellis.icor;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;
import java.util.logging.Logger;


public class LookupTableIO {

    private static Logger log = Logger.getLogger(LookupTableIO.class.getName());

    private static <T> int readInt(T in) throws IOException {
		log.fine("Read integer");
    	if (in instanceof Scanner) {         return Integer.parseInt(((Scanner)in).next()); }
    	if (in instanceof DataInputStream) { return Integer.reverseBytes(((DataInputStream)in).readInt()); }
    	throw new IOException("Error reading integer.");
    }
    
    private static <T> double[] readDoubleArray(T in) throws IOException { return readDoubleArray(in, -1); }
    private static <T> double[] readDoubleArray(T in, int size) throws IOException {
		if (size==-1) size=readInt(in);
		log.fine("Read double array of size: "+Integer.toString(size));
		double[] data=new double[size];
    	if (in instanceof Scanner) {
    		Scanner sin=(Scanner)in;
			for(int i=0; i<size; i++) data[i]=Double.parseDouble(sin.next());
    	}
    	if (in instanceof DataInputStream) {
    		DataInputStream din=(DataInputStream)in;
			for(int i=0; i<size; i++) data[i]=Double.longBitsToDouble(Long.reverseBytes(din.readLong()));
    	}
		return data; 
    }

    private static <T> int[] readShortArray(T in) throws IOException {
		int size=readInt(in);
		log.fine("Read short array of size: "+Integer.toString(size));
		int[] data=new int[size];
    	if (in instanceof Scanner) {
    		Scanner sin=(Scanner)in;
			for(int i=0; i<size; i++) data[i]=Integer.parseInt(sin.next());
    	}
    	if (in instanceof DataInputStream) {
    		DataInputStream din=(DataInputStream)in;
			for(int i=0; i<size; i++) data[i]=(int)Short.reverseBytes(din.readShort());
    	}
		return data; 
    }

    
	public static LookupTable readLUT(String resourceName) throws IOException {
		if (LookupTableIO.class.getResourceAsStream(resourceName+".bin")!=null) {
			return readLUT(LookupTableIO.class.getResourceAsStream(resourceName+".bin"),true);
		}
		if (LookupTableIO.class.getResourceAsStream(resourceName+".txt")!=null) {
			return readLUT(LookupTableIO.class.getResourceAsStream(resourceName+".txt"),false);
		}
		throw new IOException("Resource not find. None of: "+resourceName+".bin or "+resourceName+".txt");
	}

	public static LookupTable readLUT(InputStream inps, boolean is_binary) throws IOException {
		// initialize the appropriate reader
		Object in;
		if (is_binary) { in=new DataInputStream(new BufferedInputStream(inps)); }
		else           { in=new Scanner(new BufferedInputStream(inps)).useDelimiter(","); }
		
		// read the header
		int dim=readInt(in);
        int[] band = readShortArray(in);
        double[] sza = readDoubleArray(in);
        double[] vza = readDoubleArray(in);
        double[] raa = readDoubleArray(in);
        double[] height = readDoubleArray(in);
        double[] aot = readDoubleArray(in);
        double[] cwv = readDoubleArray(in);
        double[] ozone = readDoubleArray(in);

        // read the values
        int numberOfValues= readInt(in);
        int numberOfEntries = band.length * sza.length * vza.length * raa.length * height.length * aot.length * cwv.length * ozone.length;
        if (is_binary) { readInt(in); } // dummy read in binary, because it also save number of entries
        double[][] values=new double[numberOfEntries][];
        for (int i=0; i<numberOfEntries; i++) 
        	values[i]=readDoubleArray(in,numberOfValues);
        
        // return via ctor
		return new LookupTable(numberOfValues, dim, band, sza, vza, raa, height, aot, cwv, ozone, values);
	}
}
