package org.openeo.geotrellis.icor;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public abstract class ICorCorrectionDescriptor extends CorrectionDescriptor {
    private Callable lutLoader = (Callable<Broadcast<LookupTable>>) () -> JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()).broadcast(LookupTableIO.readLUT(ICorCorrectionDescriptor.this.getLookupTableURL()));
    private Broadcast<LookupTable> bcLUT;

    {
        try {
            bcLUT = AtmosphericCorrection.iCorLookupTableCache().get(this.getLookupTableURL(), lutLoader);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract String getLookupTableURL();

    /*
     * General correction function using the lookup table to convert TOA radiance to BOA reflectance
     */
    double correctRadiance(int band, double TOAradiance, double sza, double vza, double raa, double gnd, double aot, double cwv, double ozone, int waterMask) {
        final double bgRad = TOAradiance;
        final double[] params = bcLUT.value().getInterpolated(band, sza, vza, raa, gnd, aot, cwv, ozone);
        double corrected = (-1. * params[0] + params[1] * TOAradiance + params[2] * bgRad) / (params[3] + params[4] * bgRad);
        if (waterMask != 0/*==LAND*/) {
            if (waterMask == 1/*==FRESH_WATER*/) {
                corrected -= params[5];
            } else {
                corrected -= params[6];
            }
        }
        return corrected;
    }
}
