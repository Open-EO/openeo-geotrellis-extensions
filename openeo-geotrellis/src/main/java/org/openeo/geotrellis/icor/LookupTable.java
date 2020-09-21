package org.openeo.geotrellis.icor;

import java.io.File;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Sven Jochems
 */
public class LookupTable {
    
    private int numberOfValues;
    private int dimensions;
    private int[] bandids;
    private double[] sza;
    private double[] vza;
    private double[] raa;
    private double[] gnd;
    private double[] aot;
    private double[] cwv;
    private double[] ozone;
    
    double[][] values;

    public void setValues(double[][] values){
        this.values = values;
    }
    
    public void readHeader(String location) throws Exception {
        Logger log = Logger.getLogger(LookupTable.class.getName());
        String[] header = new String[]{"bandids", "sza", "vza", "raa", "gnd", "aot", "cwv", "ozon"};
        
        File lut = new File(location);
        if(!lut.exists()) {
            log.log(Level.INFO, "Lut not found: {0}", location);
        }
        
        Scanner input = new Scanner(lut);
        input.useDelimiter(",");
        
        dimensions = Integer.parseInt(input.next());
        if(dimensions != header.length) {
            String message = "Wrong header length";
            log.log(Level.SEVERE, message);
            input.close();
            throw new Exception(message);
        }
        
        // Read Header
        int NumberBands = Integer.parseInt(input.next());
        bandids = new int[NumberBands];
        for (int i = 0; i < NumberBands; i++) { bandids[i] = Integer.parseInt(input.next()); }
        
        int NumberSza = Integer.parseInt(input.next());
        sza = new double[NumberSza];
        for (int i = 0; i < NumberSza; i++) { sza[i] = Double.parseDouble(input.next()); }
        
        int NumberVza = Integer.parseInt(input.next());
        vza = new double[NumberVza];
        for (int i = 0; i < NumberVza; i++) { vza[i] = Double.parseDouble(input.next()); }
        
        int NumberRaa = Integer.parseInt(input.next());
        raa = new double[NumberRaa];
        for (int i = 0; i < NumberRaa; i++) { raa[i] = Double.parseDouble(input.next()); }
        
        int NumberGnd = Integer.parseInt(input.next());
        gnd = new double[NumberGnd];
        for (int i = 0; i < NumberGnd; i++) { gnd[i] = Double.parseDouble(input.next()); }
        
        int NumberAot = Integer.parseInt(input.next());
        aot = new double[NumberAot];
        for (int i = 0; i < NumberAot; i++) { aot[i] = Double.parseDouble(input.next()); }
        
        int NumberCwv = Integer.parseInt(input.next());
        cwv = new double[NumberCwv];
        for (int i = 0; i < NumberCwv; i++) { cwv[i] = Double.parseDouble(input.next()); }
        
        int NumberOzon = Integer.parseInt(input.next());
        ozone = new double[NumberOzon];
        for (int i = 0; i < NumberOzon; i++) { ozone[i] = Double.parseDouble(input.next()); }
                
        numberOfValues = Integer.parseInt(input.next());
        
        input.close();
    }
    
    // Read Lookup table in memory    
    public void read(String location) throws Exception {
        Logger log = Logger.getLogger(LookupTable.class.getName());
        String[] header = new String[]{"bandids", "sza", "vza", "raa", "gnd", "aot", "cwv", "ozon"};
        
        File lut = new File(location);
        if(!lut.exists()) {
            log.log(Level.INFO, "Lut not found: {0}", location);
        }
        
        Scanner input = new Scanner(lut);
        input.useDelimiter(",");
        
        dimensions = Integer.parseInt(input.next());
        if(dimensions != header.length) {
            String message = "Wrong header length";
            log.log(Level.SEVERE, message);
            input.close();
            throw new Exception(message);
        }
        
        // Read Header
        int NumberBands = Integer.parseInt(input.next());
        bandids = new int[NumberBands];
        for (int i = 0; i < NumberBands; i++) { bandids[i] = Integer.parseInt(input.next()); }
        
        int NumberSza = Integer.parseInt(input.next());
        sza = new double[NumberSza];
        for (int i = 0; i < NumberSza; i++) { sza[i] = Double.parseDouble(input.next()); }
        
        int NumberVza = Integer.parseInt(input.next());
        vza = new double[NumberVza];
        for (int i = 0; i < NumberVza; i++) { vza[i] = Double.parseDouble(input.next()); }
        
        int NumberRaa = Integer.parseInt(input.next());
        raa = new double[NumberRaa];
        for (int i = 0; i < NumberRaa; i++) { raa[i] = Double.parseDouble(input.next()); }
        
        int NumberGnd = Integer.parseInt(input.next());
        gnd = new double[NumberGnd];
        for (int i = 0; i < NumberGnd; i++) { gnd[i] = Double.parseDouble(input.next()); }
        
        int NumberAot = Integer.parseInt(input.next());
        aot = new double[NumberAot];
        for (int i = 0; i < NumberAot; i++) { aot[i] = Double.parseDouble(input.next()); }
        
        int NumberCwv = Integer.parseInt(input.next());
        cwv = new double[NumberCwv];
        for (int i = 0; i < NumberCwv; i++) { cwv[i] = Double.parseDouble(input.next()); }
        
        int NumberOzon = Integer.parseInt(input.next());
        ozone = new double[NumberOzon];
        for (int i = 0; i < NumberOzon; i++) { ozone[i] = Double.parseDouble(input.next()); }
                
        numberOfValues = Integer.parseInt(input.next());
        
        int numberOfEntries = NumberBands * NumberSza * NumberVza * NumberRaa * NumberGnd * NumberAot * NumberCwv * NumberOzon;
        
        // Read Values
        values = new double[numberOfEntries][];
        int j = 0;
        
        while (input.hasNext()){
            double[] temp = new double[numberOfValues];
            for (int i = 0; i < numberOfValues; i++) {
                 temp[i] = Double.parseDouble(input.next());
            }
            values[j++] = temp;
        }
        
        input.close();
    }
    
    
    // Get interpolated array from lookup table based on parameters
    public double[] getInterpolated(int band, double sza, double vza, double raa, double gnd, double aot, double cwv, double ozone){
        
        int[] lowerIndexes = new int[dimensions - 1];
        int[] upperIndexes = new int[dimensions - 1];
        double[] weights   = new double[dimensions - 1];
        
        // Calculate lower index, higher index and weight of each parameter
        double[] weightsAndIndexes;
        
        weightsAndIndexes = getWeightsAndIndexes(this.sza, sza);
        lowerIndexes[0]   = (int) weightsAndIndexes[0];
        upperIndexes[0]   = (int) weightsAndIndexes[1];
        weights[0]        =       weightsAndIndexes[2];
        
        weightsAndIndexes = getWeightsAndIndexes(this.vza, vza);
        lowerIndexes[1]   = (int) weightsAndIndexes[0];
        upperIndexes[1]   = (int) weightsAndIndexes[1];
        weights[1]        =       weightsAndIndexes[2];
        
        weightsAndIndexes = getWeightsAndIndexes(this.raa, raa);
        lowerIndexes[2]   = (int) weightsAndIndexes[0];
        upperIndexes[2]   = (int) weightsAndIndexes[1];
        weights[2]        =       weightsAndIndexes[2];
        
        weightsAndIndexes = getWeightsAndIndexes(this.gnd, gnd);
        lowerIndexes[3]   = (int) weightsAndIndexes[0];
        upperIndexes[3]   = (int) weightsAndIndexes[1];
        weights[3]        =       weightsAndIndexes[2];
        
        weightsAndIndexes = getWeightsAndIndexes(this.aot, aot);
        lowerIndexes[4]   = (int) weightsAndIndexes[0];
        upperIndexes[4]   = (int) weightsAndIndexes[1];
        weights[4]        =       weightsAndIndexes[2];
        
        weightsAndIndexes = getWeightsAndIndexes(this.cwv, cwv);
        lowerIndexes[5]   = (int) weightsAndIndexes[0];
        upperIndexes[5]   = (int) weightsAndIndexes[1];
        weights[5]        =       weightsAndIndexes[2];
        
        weightsAndIndexes = getWeightsAndIndexes(this.ozone, ozone);
        lowerIndexes[6]   = (int) weightsAndIndexes[0];
        upperIndexes[6]   = (int) weightsAndIndexes[1];
        weights[6]        =       weightsAndIndexes[2];
        
        return calculate(band, lowerIndexes, upperIndexes, weights);
    }
    
    /**
     * Get lower index, upper index and weight of value in list
     * @param list  double[]: values from lookup table
     * @param value double: value
     * @return double[3]{lower index, higher index, weight}
     */
    private double[] getWeightsAndIndexes(double[] list, double value) {
        
        if(list.length == 1) return new double[]{0.,0.,0.};
        
        int indexLow  = -1;
        int indexHigh = 0;
        
        for (int i = 0; i < list.length; i++) {
            if (list[i] < value) {
                    indexLow++;
                    indexHigh++;
            }             
        }
        
        if (indexLow < 0) indexLow += 1;
        if (indexHigh >= list.length) indexHigh -= 1;
        
        double weight;
        if ( indexLow == indexHigh )
            weight = 0.0; 
        else
            weight = (value - list[indexLow]) / (list[indexHigh] - list[indexLow]);
        
        return new double[]{indexLow, indexHigh, weight};
    }
    
    // Calculate interpolated value from lookuptable based on lower indexes, higher indexes and weights
    private double[] calculate(int band, int[] lows, int[] highs, double[] weights) {
        
        double[] w      = new double[lows.length];
        double[] result = new double[numberOfValues];
        double[] val;
        double total_weight;
        
        for (int i = lows[0]; i <= highs[0] ; i++) {
            w[0] = i != lows[0] ? weights[0] : 1 - weights[0]; 
            
            for (int j = lows[1]; j <= highs[1] ; j++) {
                w[1] = j != lows[1] ? weights[1] : 1 - weights[1];
                
                for (int k = lows[2]; k <= highs[2] ; k++) {
                    w[2] = k != lows[2] ? weights[2] : 1 - weights[2];
                    
                    for (int l = lows[3]; l <= highs[3] ; l++) {
                        w[3] = l != lows[3] ? weights[3] : 1 - weights[3];
                        
                        for (int m = lows[4]; m <= highs[4] ; m++) {
                            w[4] = m != lows[4] ? weights[4] : 1 - weights[4]; 
                            
                            for (int n = lows[5]; n <= highs[5] ; n++) {
                                w[5] = n != lows[5] ? weights[5] : 1 - weights[5];
                                
                                for (int o = lows[6]; o <= highs[6] ; o++) {
                                    w[6] = o != lows[6] ? weights[6] : 1 - weights[6]; 
                                    
                                    total_weight = 1.;
                                    
                                    for (double x : w){ total_weight *= x; }
                                    
                                    val = get(band, i, j, k, l, m, n, o);
                                    
                                    for (int p = 0; p < val.length -1; p++) {
                                        result[p] += total_weight * val[p];                                        
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }        
        
        return result;
    }
    
    // Get entry from lookup table based on indexes
    private double[] get(int band, int sza, int vza, int raa, int gnd, int aot, int cwv, int ozone){
        
        int position = band * this.sza.length * this.vza.length * this.raa.length * this.gnd.length * this.aot.length * this.cwv.length * this.ozone.length;
        position += sza * this.vza.length * this.raa.length * this.gnd.length * this.aot.length * this.cwv.length * this.ozone.length;
        position += vza * this.raa.length * this.gnd.length * this.aot.length * this.cwv.length * this.ozone.length;
        position += raa * this.gnd.length * this.aot.length * this.cwv.length * this.ozone.length;
        position += gnd * this.aot.length * this.cwv.length * this.ozone.length;
        position += aot * this.cwv.length * this.ozone.length;
        position += cwv * this.ozone.length;
        position += ozone;
                
        return values[position];
    }    
}
