import java.util.Arrays;

import geotrellis.spark.SpatialKey;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.openeo.geotrellisseeder.Band;
import org.openeo.geotrellisseeder.MemoryLogger;
import org.openeo.geotrellisseeder.TileSeeder;
import scala.Option;

import static java.lang.String.join;

public class TileSeederRunner {
    
    public static void main(String... args) {
        MemoryLogger ml = new MemoryLogger("main");
        
        Option<Band[]> bands = Option.<Band[]>empty();
        Option<String> colorMap = Option.<String>empty();
        if (args.length > 4) {
            bands = Option.<Band[]>apply(Arrays.stream(args[4].split(":")).map(Band::apply).toArray(Band[]::new));
        } else if (args.length > 3) {
            colorMap = Option.<String>apply(args[3]);
        }
        
        if (args.length > 1) {
            String productType = args[0];
            String rootPath = args[1];
            String date = args[2];

            SparkContext sc = SparkContext.getOrCreate(
                    new SparkConf()
                            .setMaster("local[8]")
                            .setAppName(join(":", "GeotrellisSeeder", productType, date))
                            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                            .set("spark.kryoserializer.buffer.max", "1024m"));
            
            new TileSeeder(13, 500, false).renderPng(rootPath, productType, date, colorMap, bands, Option.<SpatialKey>empty(), sc);
        }
        
        ml.logMem();
    }
}
