package org.openeo.geotrelliscommon;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BatchJobMetadataTracker implements Serializable {
    public static String SH_PU = "Sentinelhub_Processing_Units";

    private static Optional<Boolean> forceTracking = Optional.empty();
    public static void setGlobalTracking(boolean enable){
        forceTracking = Optional.of(enable);
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
        public void add(String name, double value) {

        }

        @Override
        public Map<String, Object> asDict() {
            return Collections.emptyMap();
        }
    };

    public static BatchJobMetadataTracker tracker(String id) {
        if((forceTracking.isPresent() && forceTracking.get()) || (!forceTracking.isPresent() && System.getenv().containsKey("OPENEO_BATCH_JOB_ID"))){
            return trackers.getOrDefault(id, defaultTracker);
        }else{
            return dummyTracker;
        }
    }

    public abstract void registerCounter(String name);

    public abstract void registerDoubleCounter(String name);

    public abstract void add(String name, double value);

    public abstract Map<String, Object> asDict();
}
