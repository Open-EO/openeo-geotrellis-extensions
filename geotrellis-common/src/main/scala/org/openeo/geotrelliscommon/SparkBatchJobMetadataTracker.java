package org.openeo.geotrelliscommon;

import org.apache.spark.SparkContext;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Function0;
import scala.Function1;
import scala.Option;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkBatchJobMetadataTracker extends BatchJobMetadataTracker {

    private Map<String, AccumulatorV2<Long, Long>> counters = new HashMap<>();
    private Map<String, AccumulatorV2<Double, Double>> doubleCounters = new HashMap<>();
    private Map<String, List<ProductIdAndUrl>> inputProducts = new HashMap<>();
    private List<InternalFile> internalFiles = new ArrayList<>();

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
    public void addInputProductsWithUrls(String collection, List<ProductIdAndUrl> productIdAndUrls) {
        inputProducts.merge(collection, productIdAndUrls,(v1, v2) -> Stream.concat(v1.stream(),v2.stream()).collect(Collectors.toList()));
    }

    @Override
    public void addInternalFile(Function0<Path> writer, String mediaType) {
        internalFiles.add(new InternalFile(writer.apply(), mediaType));
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
        result.put("internal_files", internalFiles);
        return result;
    }
}
