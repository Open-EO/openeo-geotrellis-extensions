import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.openeo.geotrellisseeder.Band;
import org.openeo.geotrellisseeder.MemoryLogger;
import org.openeo.geotrellisseeder.TileSeeder;
import scala.Option;

import java.util.Arrays;

import static java.lang.String.join;

public class TileSeederRunner {

    public static void main(String... args) {
        MemoryLogger ml = new MemoryLogger("main");

        Option<Band[]> bands = Option.empty();
        Option<String> colorMap = Option.empty();
        if (args.length > 4) {
            bands = Option.apply(Arrays.stream(args[4].split(":")).map(Band::apply).toArray(Band[]::new));
        } else if (args.length > 3) {
            colorMap = Option.apply(args[3]);
        }

        Option<String> productGlob = Option.apply("/data/MTDA/TERRASCOPE_Sentinel2/FCOVER_V2/#DATE#/S2*_FCOVER_V200/10M/S2*_FCOVER_10M_V200.tif");
        if (args.length > 5)
            productGlob = Option.apply(args[5]);

        int[] maskValues = new int[0];
        if (args.length > 6)
            maskValues = Arrays.stream(args[6].split(",")).mapToInt(Integer::valueOf).toArray();

        Option<String> permissions = Option.empty();
        if (args.length > 7)
            permissions = Option.apply(args[7]);

        if (args.length > 1) {
            String productType = args[0];
            String rootPath = args[1];
            String date = args[2];

            SparkContext sc = SparkContext.getOrCreate(
                    new SparkConf()
                            .setMaster("local[2]")
                            .setAppName(join(":", "GeotrellisSeeder", productType, date))
                            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                            .set("spark.kryoserializer.buffer.max", "1024m"));

            new TileSeeder(5, false, Option.empty())
                    .renderPng(rootPath, productType, date, colorMap, bands, productGlob, maskValues, permissions, Option.empty(), Option.empty(), Option.empty(), Option.empty(), Option.empty(), Option.empty(), Option.empty(),sc);
        }

        ml.logMem();
    }
}
