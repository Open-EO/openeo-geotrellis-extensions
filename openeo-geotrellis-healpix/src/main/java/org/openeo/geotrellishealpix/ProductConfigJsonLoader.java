package org.openeo.geotrellishealpix;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import scala.Option;
import scala.collection.immutable.Map;
import scala.collection.immutable.Seq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;

/** * Load a {@link Sentinel3BinningReader.ProductConfig} from a JSON representation. * * === JSON Format === * * <pre> * { *   "latVariable": "latitude", *   "lonVariable": "longitude", *   "assetVariables": { *     "instrument_data": ["LST", "uncertainty"], *     "geo_coordinates": ["latitude", "longitude"], *     "flags": ["quality_flags"] *   }, *   "fillValue": -9999.0, *   "geoFileSuffix": "geo_coordinates.nc", *   "s3Endpoint": "eodata.dataspace.copernicus.eu" * } * </pre> * * All fields are optional except assetVariables (must have at least one asset): * - latVariable: "latitude" * - lonVariable: "longitude" * - assetVariables: {} (must be provided with at least one asset) * - fillValue: NaN * - geoFileSuffix: null (absence means None) * - s3Endpoint: "eodata.dataspace.copernicus.eu" */
public class ProductConfigJsonLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**     * Load configuration from a JSON string.     *     * @param json JSON string containing the configuration     * @return a ProductConfig instance     * @throws IOException if JSON parsing fails     * @throws IllegalArgumentException if the configuration is invalid     */
    public static Sentinel3BinningReader.ProductConfig fromJson(String json)
            throws IOException {
        return fromJsonNode(MAPPER.readTree(json));
    }

    /**     * Load configuration from a JSON file.     *     * @param path path to the JSON file     * @return a ProductConfig instance     * @throws IOException if file reading or JSON parsing fails     * @throws IllegalArgumentException if the configuration is invalid     */
    public static Sentinel3BinningReader.ProductConfig fromJsonFile(Path path)
            throws IOException {
        String json = Files.readString(path, StandardCharsets.UTF_8);
        return fromJson(json);
    }

    /**     * Load configuration from a JsonNode (already parsed).     *     * @param node the JsonNode to load from     * @return a ProductConfig instance     * @throws IllegalArgumentException if the configuration is invalid     */
    public static Sentinel3BinningReader.ProductConfig fromJsonNode(JsonNode node) {
        // Extract and validate assetVariables (required)
        java.util.Map<String, java.util.List<String>> assetVariablesMap = new HashMap<>();

        if (!node.has("assetVariables")) {
            throw new IllegalArgumentException(
                    "assetVariables is required"
            );
        }

        JsonNode avNode = node.get("assetVariables");
        if (!avNode.isObject()) {
            throw new IllegalArgumentException(
                    "assetVariables must be an object mapping asset names to variable lists"
            );
        }

        avNode.fields().forEachRemaining(entry -> {
            String assetName = entry.getKey();
            JsonNode varsNode = entry.getValue();
            if (!varsNode.isArray()) {
                throw new IllegalArgumentException(
                        "assetVariables['" + assetName + "'] must be an array of strings"
                );
            }
            java.util.List<String> vars = new ArrayList<>();
            varsNode.forEach(varNode -> vars.add(varNode.asText()));
            if (vars.isEmpty()) {
                throw new IllegalArgumentException(
                        "assetVariables['" + assetName + "'] cannot be empty"
                );
            }
            assetVariablesMap.put(assetName, vars);
        });

        if (assetVariablesMap.isEmpty()) {
            throw new IllegalArgumentException(
                    "assetVariables must have at least one asset"
            );
        }

        // Convert Java Map<String, List<String>> to Scala immutable Map
        Map<String, Seq<String>> scalaAssetMap = ConfigJsonLoader.javaMapToScalaMap(assetVariablesMap);

        // Extract optional fields with defaults
        String latVariable = node.has("latVariable")
                ? node.get("latVariable").asText()
                : "latitude";

        String lonVariable = node.has("lonVariable")
                ? node.get("lonVariable").asText()
                : "longitude";

        double fillValue = node.has("fillValue")
                ? node.get("fillValue").asDouble(Double.NaN)
                : Double.NaN;

        String s3Endpoint = node.has("s3Endpoint")
                ? node.get("s3Endpoint").asText()
                : "eodata.dataspace.copernicus.eu";

        // Extract optional String: geoFileSuffix
        Option<String> geoFileSuffixOpt;
        if (node.has("geoFileSuffix")) {
            JsonNode gsNode = node.get("geoFileSuffix");
            if (!gsNode.isNull()) {
                geoFileSuffixOpt = Option.apply(gsNode.asText());
            } else {
                geoFileSuffixOpt = Option.empty();
            }
        } else {
            geoFileSuffixOpt = Option.empty();
        }

        // Construct the Scala ProductConfig directly
        return new Sentinel3BinningReader.ProductConfig(
                latVariable,
                lonVariable,
                scalaAssetMap,
                fillValue,
                geoFileSuffixOpt,
                s3Endpoint
        );
    }


    /**     * Load configuration from a Jackson JsonNode wrapped in a JSON object.     * Useful when the configuration is nested inside another JSON structure.     *     * @param parentNode parent JSON object     * @param fieldName name of the field containing the ProductConfig JSON     * @return a ProductConfig instance     * @throws IllegalArgumentException if the field doesn't exist or is invalid     */
    public static Sentinel3BinningReader.ProductConfig fromJsonNode(
            JsonNode parentNode, String fieldName) {
        if (!parentNode.has(fieldName)) {
            throw new IllegalArgumentException(
                    "Field '" + fieldName + "' not found in JSON"
            );
        }
        return fromJsonNode(parentNode.get(fieldName));
    }
}