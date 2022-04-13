package org.openeo.geotrelliscommon;

import org.apache.spark.SparkContext;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import java.util.HashMap;
import java.util.Map;

public class SparkBatchJobMetadataTracker extends BatchJobMetadataTracker {

    private Map<String, AccumulatorV2<Long, Long>> counters = new HashMap<>();
    private Map<String, AccumulatorV2<Double, Double>> doubleCounters = new HashMap<>();

    @Override
    public void registerCounter(String name) {
        if (!counters.containsKey(name)) {
            LongAccumulator acc = SparkContext.getOrCreate().longAccumulator(name);
            counters.put(name, acc);
        }
    }

    @Override
    public void registerDoubleCounter(String name) {
        if (!doubleCounters.containsKey(name)) {
            DoubleAccumulator acc = SparkContext.getOrCreate().doubleAccumulator(name);
            doubleCounters.put(name, acc);
        }
    }

    @Override
    public void add(String name, double value) {
        doubleCounters.get(name).add(value);
    }

    @Override
    public Map<String, Object> asDict() {
        Map<String, Object> result = new HashMap<>();
        doubleCounters.forEach((key, value) -> {
            result.put(key, value.value());
        });
        counters.forEach((key, value) -> {
            result.put(key, value.value());
        });
        return result;
    }
}
