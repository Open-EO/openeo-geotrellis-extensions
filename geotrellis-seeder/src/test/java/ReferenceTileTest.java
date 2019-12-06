import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import geotrellis.layer.SpatialKey;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openeo.geotrellisseeder.Band;
import org.openeo.geotrellisseeder.TileSeeder;
import scala.Option;
import scala.Some;

import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.toList;

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
                        .set("spark.driver.bindAddress", "127.0.0.1")
                        .setAppName("ReferenceTileTest")
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        .set("spark.kryoserializer.buffer.max", "1024m"));
        
        seeder = new TileSeeder(13, false, Option.apply(1));
    }

    @AfterClass
    public static void shutDownSparkContext() {
        sc.stop();
    }

    @Test
    public void testSaveAndCompareAll() throws ImageComparisonFailedException, IOException, InterruptedException {
        for (Layers layer: Layers.values()) {
            Path layerDir = layer.getDir();
            
            List<Path> refs = Files.find(layerDir, 1, (path, fileAttributes) -> path.toString().contains("ref")).collect(toList());
            for (Path ref : refs) {
                String fileName = ref.getFileName().toString();
                String[] fileParts = fileName.split("_");
               
                LocalDate date = LocalDate.parse(fileParts[1]);
                
                Pattern p = Pattern.compile("(\\d*)x(\\d*)\\.png");
                Matcher m = p.matcher(fileParts[2]);
                if (m.find()) {
                    SpatialKey key = SpatialKey.apply(parseInt(m.group(1)), parseInt(m.group(2)));

                    Path refFile = layerDir.resolve(fileName);

                    Path newFile = layerDir.resolve(fileName.replace("ref", "actual"));
                    layer.generateTile(newFile.toString(), date, key);

                    Path diffFile = layerDir.resolve(fileName.replace("ref", "diff"));

                    compare(newFile, refFile, diffFile);
                }
            }
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
            void generateTile(String path, LocalDate date, SpatialKey key) {
                Option<String> colorMap = Some.apply("ColorTable_NDVI_V2.sld");
                Option<Band[]> bands = Option.empty();

                seeder.renderSinglePng(name(), date, key, path, colorMap, bands, sc);
            }
        },
        CGS_S2_LAI {
            @Override
            void generateTile(String path, LocalDate date, SpatialKey key) {
                Option<String> colorMap = Some.apply("ColorTable_LAI_V12.sld");
                Option<Band[]> bands = Option.empty();

                seeder.renderSinglePng(name(), date, key, path, colorMap, bands, sc);
            }
        },
        CGS_S2_NDVI {
            @Override
            void generateTile(String path, LocalDate date, SpatialKey key) {
                Option<String> colorMap = Some.apply("ColorTable_NDVI_PROBAV.sld");
                Option<Band[]> bands = Option.empty();

                seeder.renderSinglePng(name(), date, key, path, colorMap, bands, sc);
            }
        },
        CGS_S2_FCOVER {
            @Override
            void generateTile(String path, LocalDate date, SpatialKey key) {
                Option<String> colorMap = Some.apply("ColorTable_FCOVER_V12.sld");
                Option<Band[]> bands = Option.empty();

                seeder.renderSinglePng(name(), date, key, path, colorMap, bands, sc);
            }
        },
        CGS_S2_RADIOMETRY {
            @Override
            void generateTile(String path, LocalDate date, SpatialKey key) {
                Option<String> colorMap = Some.empty();
                Option<Band[]> bands = Option.apply(new Band[] { 
                        Band.apply("B04", 200, 1600), 
                        Band.apply("B03", 200, 1600), 
                        Band.apply("B02", 200, 1600) 
                });

                seeder.renderSinglePng(name(), date, key, path, colorMap, bands, sc);
            }
        },
        CGS_S2_NIR {
            @Override
            void generateTile(String path, LocalDate date, SpatialKey key) {
                Option<String> colorMap = Some.empty();
                Option<Band[]> bands = Option.apply(new Band[] {
                        Band.apply("B08", 0, 4000),
                        Band.apply("B04", 0, 2600),
                        Band.apply("B03", 0, 2600)
                });

                seeder.renderSinglePng(CGS_S2_RADIOMETRY.name(), date, key, path, colorMap, bands, sc);
            }
        };
        
        abstract void generateTile(String target, LocalDate date, SpatialKey key);
        
        Path getDir() {
            return Paths.get(REFERENCE_IMAGES_DIR, name());   
        }
    }
}
