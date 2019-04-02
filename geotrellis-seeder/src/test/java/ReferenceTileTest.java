import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;

import geotrellis.spark.SpatialKey;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openeo.geotrellisvlm.TileSeeder;
import scala.Option;
import scala.Some;

public class ReferenceTileTest {
    
    private static final String REFERENCE_IMAGES_DIR = "/data/TERRASCOPE/automated_test_files/geotrellis-seeder";

    private static final String COMPARE_SCRIPT= "/compare/compare.sh";

    private static SparkContext sc;

    @BeforeClass
    public static void createSparkContext() {
        sc = SparkContext.getOrCreate(
                new SparkConf()
                        .setMaster("local[1]")
                        .setAppName("ReferenceTileTest")
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        .set("spark.kryoserializer.buffer.max", "1024m"));
    }


    @Test
    public void testSaveAndCompareAll() throws ImageComparisonFailedException, IOException, InterruptedException {
        for (Layers layer: Layers.values()) {
            Path layerDir = layer.getDir();
            
            Path refFile = layerDir.resolve("ref.png");
            
            Path newFile = layerDir.resolve("actual.png");
            layer.generateTile(newFile.toString());

            Path diffFile = layerDir.resolve("diff.png");
            
            compare(newFile, refFile, diffFile);
        }
    }

    private void compare(Path newFile, Path referenceFile, Path diffFile) throws ImageComparisonFailedException, IOException, InterruptedException {
        Process compareScript = new ProcessBuilder("bash", absolutePath(COMPARE_SCRIPT), newFile.toString(), referenceFile.toString(), diffFile.toString()).start();
        int exitValue = compareScript.waitFor();

        switch (exitValue) {
            case 0:
                try { Files.delete(newFile); } catch (IOException ignore) {}
                try { Files.delete(diffFile); } catch (IOException ignore) {}
                break;
            case 1:
                throw new ImageComparisonFailedException(String.format("%s does not match reference image %s; diff has been saved to %s", newFile, referenceFile, diffFile));
            default:
                String errorMessage = IOUtils.toString(compareScript.getErrorStream());
                throw new ImageComparisonFailedException(String.format("could not compare %s to reference image %s: %s", newFile, referenceFile, errorMessage));
        }
    }

    private static String absolutePath(String classPathResource) {
        URL url = ReferenceTileTest.class.getResource(classPathResource);
        try {
            return Paths.get(url.toURI()).toString();
        } catch (URISyntaxException e) {
            throw new AssertionError("could not convert URL from Class#getResource to URI");
        }
    }

    private static class ImageComparisonFailedException extends Exception {
        ImageComparisonFailedException(String message) {
            super(message);
        }
    }

    private enum Layers {
        CGS_S2_FAPAR {
            @Override
            void generateTile(String path) {
                LocalDate date = LocalDate.of(2019, 3, 3);
                SpatialKey key = SpatialKey.apply(8660, 5978);
                Option<String> colorMap = Some.apply("styles_ColorTable_NDVI_V2.sld");
                
                TileSeeder.renderSinglePng(name(), date, key, path, colorMap, sc);
            }
        },
        CGS_S2_LAI {
            @Override
            void generateTile(String path) {
                LocalDate date = LocalDate.of(2019, 3, 1);
                SpatialKey key = SpatialKey.apply(13036, 6311);
                Option<String> colorMap = Some.apply("styles_ColorTable_LAI_V12.sld");

                TileSeeder.renderSinglePng(name(), date, key, path, colorMap, sc);
            }
        },
        CGS_S1_GRD_SIGMA0_L1 {
            @Override
            void generateTile(String path) {
                LocalDate date = LocalDate.of(2019, 3, 3);
                SpatialKey key = SpatialKey.apply(8274, 5584);

                TileSeeder.renderSinglePng(name(), date, key, path, Option.empty(), sc);
            }
        };
        
        abstract void generateTile(String target);
        
        Path getDir() {
            return Paths.get(REFERENCE_IMAGES_DIR, name());   
        }
    }
}
