package org.openeo.geotrelliscommon;

import org.apache.spark.SparkContext;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkBatchJobMetadataTracker extends BatchJobMetadataTracker {

    private Map<String, AccumulatorV2<Long, Long>> counters = new HashMap<>();
    private Map<String, AccumulatorV2<Double, Double>> doubleCounters = new HashMap<>();
    private Map<String, List<String>> inputProducts = new HashMap<>();

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
    public void add(String name, long value) {
        counters.get(name).add(value);
    }

    @Override
    public void add(String name, double value) {
        doubleCounters.get(name).add(value);
    }

    @Override
    public void addInputProducts(String collection, List<String> productIds) {
        inputProducts.merge(collection, productIds,(v1, v2) -> Stream.concat(v1.stream(),v2.stream()).collect(Collectors.toList()));
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
        //needs to go under 'derived-from links
        result.put("links", inputProducts);
        return result;
    }
}
