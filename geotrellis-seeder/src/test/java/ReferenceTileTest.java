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
import org.openeo.geotrellisseeder.Band;
import org.openeo.geotrellisseeder.TileSeeder;
import scala.Option;
import scala.Some;

public class ReferenceTileTest {
    
    private static final String REFERENCE_IMAGES_DIR = "/data/TERRASCOPE/automated_test_files/geotrellis-seeder";

    private static final String COMPARE_SCRIPT= "/compare/compare.sh";

    private static SparkContext sc;
    
    private static TileSeeder seeder;

    @BeforeClass
    public static void createSparkContextAndSeeder() {
        sc = SparkContext.getOrCreate(
                new SparkConf()
                        .setMaster("local[1]")
                        .setAppName("ReferenceTileTest")
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        .set("spark.kryoserializer.buffer.max", "1024m"));
        
        seeder = new TileSeeder(13, 1, false);
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
                SpatialKey key = SpatialKey.apply(4330, 2989);
                Option<String> colorMap = Some.<String>apply("styles_ColorTable_NDVI_V2.sld");
                Option<Band[]> bands = Option.<Band[]>empty();

                seeder.renderSinglePng(name(), date, key, path, colorMap, bands, sc);
            }
        },
        CGS_S2_LAI {
            @Override
            void generateTile(String path) {
                LocalDate date = LocalDate.of(2019, 3, 28);
                SpatialKey key = SpatialKey.apply(4046, 2673);
                Option<String> colorMap = Some.<String>apply("styles_ColorTable_LAI_V12.sld");
                Option<Band[]> bands = Option.<Band[]>empty();

                seeder.renderSinglePng(name(), date, key, path, colorMap, bands, sc);
            }
        },
        CGS_S2_RADIOMETRY {
            @Override
            void generateTile(String path) {
                LocalDate date = LocalDate.of(2019, 4, 1);
                SpatialKey key = SpatialKey.apply(4209, 2803);
                Option<String> colorMap = Some.<String>empty();
                Option<Band[]> bands = Option.<Band[]>apply(new Band[] { 
                        Band.apply("B04", 200, 1600), 
                        Band.apply("B03", 200, 1600), 
                        Band.apply("B02", 200, 1600) 
                });

                seeder.renderSinglePng(name(), date, key, path, colorMap, bands, sc);
            }
        },
        CGS_S2_NIR {
            @Override
            void generateTile(String path) {
                LocalDate date = LocalDate.of(2019, 4, 1);
                SpatialKey key = SpatialKey.apply(4209, 2803);
                Option<String> colorMap = Some.<String>empty();
                Option<Band[]> bands = Option.<Band[]>apply(new Band[] {
                        Band.apply("B08", 0, 4000),
                        Band.apply("B04", 0, 2600),
                        Band.apply("B03", 0, 2600)
                });

                seeder.renderSinglePng(CGS_S2_RADIOMETRY.name(), date, key, path, colorMap, bands, sc);
            }
        };
        
        abstract void generateTile(String target);
        
        Path getDir() {
            return Paths.get(REFERENCE_IMAGES_DIR, name());   
        }
    }
}
