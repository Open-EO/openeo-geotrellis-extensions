import java.time.LocalDate;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.openeo.geotrellisvlm.LoadSigma0;
import org.openeo.geotrellisvlm.MemoryLogger;
import scala.Option;

public class LoadSigma0Runner {
    
    public static void main(String... args) {
        MemoryLogger ml = new MemoryLogger("main");

        SparkContext sc = SparkContext.getOrCreate(
                new SparkConf()
                        .setMaster("local[8]")
                        .setAppName("Geotiffloading")
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        .set("spark.kryoserializer.buffer.max", "1024m"));

        Option<String> colorMap = Option.apply(null);
        if (args.length > 2) {
            colorMap = Option.apply(args[2]);
        }
        
        if (args.length > 1) {
            String productType = args[0];
            LocalDate date = LocalDate.parse(args[1]);
            
            LoadSigma0.renderPng(productType, date, colorMap, sc);
        }
        
        ml.logMem();
    }
}
