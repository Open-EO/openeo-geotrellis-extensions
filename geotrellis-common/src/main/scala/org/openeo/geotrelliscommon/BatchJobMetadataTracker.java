package org.openeo.geotrelliscommon;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BatchJobMetadataTracker implements Serializable {
    public static class ProductIdAndUrl implements Serializable {
        public ProductIdAndUrl(String id, String selfUrl) {
            assert id != null;
            this.id = id;
            this.selfUrl = selfUrl;
        }

        private final String id;
        private final String selfUrl;

        public String getId() {
            return id;
        }

        public String getSelfUrl() {
            return selfUrl != null ? selfUrl : id;
        }

        @Override
        public String toString() {
            return String.format("%s: %s", id, selfUrl);
        }
    }

    public static String SH_PU = "Sentinelhub_Processing_Units";
    public static String SH_FAILED_TILE_REQUESTS = "Sentinelhub_Failed_Tile_Requests";

    private static Optional<Boolean> forceTracking = Optional.empty();
    public static void setGlobalTracking(boolean enable){
        forceTracking = Optional.of(enable);
    }

    public static void clearGlobalTracker() {
        defaultTracker = new SparkBatchJobMetadataTracker();
    }
    private static Map<String, SparkBatchJobMetadataTracker> trackers = new ConcurrentHashMap<>();
    private static SparkBatchJobMetadataTracker defaultTracker = new SparkBatchJobMetadataTracker();
    private static BatchJobMetadataTracker dummyTracker = new BatchJobMetadataTracker() {

        @Override
        public void registerCounter(String name) {

        }

        @Override
        public void registerDoubleCounter(String name) {

        }

        @Override
        public void add(String name, long value) {

        }

        @Override
        public void add(String name, double value) {

        }

        @Override
        public void addInputProductsWithUrls(String collection, List<ProductIdAndUrl> productIdAndUrls) {}

        @Override
        public Map<String, Object> asDict() {
            return Collections.emptyMap();
        }
    };

    public static BatchJobMetadataTracker tracker(String id) {
        if((forceTracking.isPresent() && forceTracking.get()) || (!forceTracking.isPresent() && System.getenv().containsKey("OPENEO_BATCH_JOB_ID"))){
            return trackers.getOrDefault(id, defaultTracker); // TODO: nothing is ever put into this map so will always return defaultTracker
        }else{
            return dummyTracker;
        }
    }

    public abstract void registerCounter(String name);

    public abstract void registerDoubleCounter(String name);

    public abstract void add(String name, long value);

    public abstract void add(String name, double value);

    public void addInputProducts(String collection, List<String> productIds) {
        List<ProductIdAndUrl> productIdAndUrls = new ArrayList<ProductIdAndUrl>();
        for (String id : productIds) {
            productIdAndUrls.add(new ProductIdAndUrl(id, null));
        }
        this.addInputProductsWithUrls(collection, productIdAndUrls);
    }

    /**
     * Different name than 'addInputProducts' to avoid "both methods have same erasure" compiler error.
     */
    public abstract void addInputProductsWithUrls(String collection, List<ProductIdAndUrl> productIdAndUrls);

    public abstract Map<String, Object> asDict();
}
