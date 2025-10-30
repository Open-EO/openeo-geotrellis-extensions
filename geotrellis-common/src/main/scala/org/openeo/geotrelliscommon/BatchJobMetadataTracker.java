package org.openeo.geotrelliscommon;

import scala.Function0;
import scala.Option;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    public static class AuxiliaryFile implements Serializable {
        private final String path;
        private final String mediaType;

        public AuxiliaryFile(Path path, String mediaType) {
            this.path = path.toString();
            this.mediaType = mediaType;
        }

        public Path getPath() {
            return Paths.get(path);
        }

        public String getMediaType() {
            return mediaType;
        }
    }

    public static final String SH_PU = "Sentinelhub_Processing_Units";
    public static final String SH_FAILED_TILE_REQUESTS = "Sentinelhub_Failed_Tile_Requests";
    public static final String AUXILIARY_FILES = "auxiliary_files";

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
        public void addAuxiliaryFile(Function0<Path> writer, String mediaType) {}

        @Override
        public Map<String, Object> asDict() {
            return Collections.emptyMap();
        }
    };

    public static BatchJobMetadataTracker tracker(String id) {
        if ((forceTracking.isPresent() && forceTracking.get()) || (forceTracking.isEmpty() && getBatchJobId().nonEmpty())) {
            return trackers.getOrDefault(id, defaultTracker); // TODO: nothing is ever put into this map so will always return defaultTracker
        } else {
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

    public void addAuxiliaryFile(AuxiliaryFileWriter writer, String mediaType) {
        /* "thunking" by means of an AuxiliaryFileWriter avoid the creation of these files in a sync context by putting
        the decision in the hands of the BatchJobMetadataTracker implementation */
        addAuxiliaryFile(() -> writer.write(getBatchJobId()), mediaType);
    }

    protected abstract void addAuxiliaryFile(Function0<Path> writer, String mediaType);

    @SuppressWarnings("unused")
    public void addAuxiliaryFile(String path, String mediaType) {
        /* convenience function for Python unit tests */
        addAuxiliaryFile((jobId) -> Paths.get(path), mediaType);
    }

    private static Option<String> getBatchJobId() {
        return Option.apply(System.getenv("OPENEO_BATCH_JOB_ID"));
    }

    public abstract Map<String, Object> asDict();
}
