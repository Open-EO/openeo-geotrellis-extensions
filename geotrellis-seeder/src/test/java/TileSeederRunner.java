import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.openeo.geotrellisvlm.MemoryLogger;
import org.openeo.geotrellisvlm.TileSeeder;
import scala.Option;

import static java.lang.String.join;

public class TileSeederRunner {
    
    public static void main(String... args) {
        MemoryLogger ml = new MemoryLogger("main");

        Option<String> colorMap = Option.empty();
        if (args.length > 3) {
            colorMap = Option.apply(args[3]);
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
            
            TileSeeder.renderPng(rootPath, productType, date, colorMap, Option.empty(), sc);
        }
        
        ml.logMem();
    }
}
