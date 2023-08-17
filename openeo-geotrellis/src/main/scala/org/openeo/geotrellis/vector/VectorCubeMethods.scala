package org.openeo.geotrellis.vector

import io.circe.{Json, JsonObject}
import io.circe.parser
import geotrellis.layer.{KeyBounds, LayoutDefinition, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{ArrayTile, CellSize, DoubleArrayFiller, DoubleConstantNoDataCellType, GridExtent, MultibandTile, NODATA, PixelIsPoint, RasterExtent, Tile}
import geotrellis.vector.{io, _}
import org.apache.spark.SparkContext
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.rasterize.Rasterizer.foreachCellByGeometry
import geotrellis.spark.rasterize.RasterizeRDD.fromKeyedFeature
import geotrellis.spark.{MultibandTileLayerRDD, withFeatureClipToGridMethods}
import org.apache.spark.rdd.RDD

object VectorCubeMethods {

  def rasterizeWithDouble(geom: Geometry, rasterExtent: RasterExtent, value: Double): Tile =
    if(geom.isValid) {
      val cols = rasterExtent.cols
      val array = Array.ofDim[Double](rasterExtent.cols * rasterExtent.rows).fill(NODATA)
      val f2 = (col: Int, row: Int) => array(row * cols + col) = value
      foreachCellByGeometry(geom, rasterExtent)(f2)
      ArrayTile(array, rasterExtent.cols, rasterExtent.rows)
  } else throw new IllegalArgumentException("Cannot rasterize an invalid polygon")


  /**
   * Convert a vector datacube to a raster datacube.
   *
   * @param path: Path to the geojson file.
   * @param target_resolution: Target resolution of the raster datacube.
   * @param target_crs: Target CRS of the raster datacube.
   * @return A raster datacube.
  */
  def vectorToRaster(path: String, target_resolution: Integer, target_crs: String): MultibandTileLayerRDD[SpatialKey] = {
    val sc = SparkContext.getOrCreate()

    val source = scala.io.Source.fromFile(path)
    val sourceString = try source.mkString finally source.close()
    val json = parser.parse(sourceString).getOrElse(Json.Null)
    val listJson: List[Json] = json.hcursor.downField("features").as[List[Json]].getOrElse(throw new Exception("No features found in GeoJSON"))
    val features: Seq[Feature[Geometry, Double]] = listJson.map({ json: Json =>
      val geometry: Geometry = json.hcursor.downField("geometry").as[Geometry].getOrElse(throw new Exception("No geometry found in GeoJSON"))
      val propertiesMap = json.hcursor.downField("properties").as[JsonObject].getOrElse(JsonObject.empty).toMap
      val properties: Array[Double] = propertiesMap.values.map({ value =>
        value.asNumber match {
          case Some(number) => number.toDouble
          case None => Double.NaN
        }
      }).toArray[Double]
      // TODO: Add all properties as separate bands.
      Feature(geometry, properties.head)
    })

    val featuresRDD: RDD[Feature[Geometry, Double]] = sc.parallelize(features)
    val extent = featuresRDD.map(_.geom.extent).reduce(_.combine(_))
    // TODO: Use layoutdefinition from target_datacube if provided.
    val layoutDefinition = LayoutDefinition(
      GridExtent[Long](extent, CellSize(target_resolution.toInt, target_resolution.toInt)),
      256
    )
    val keyedFeatures: RDD[(SpatialKey, Feature[Geometry, Double])] = featuresRDD.clipToGrid(layoutDefinition)

    val options = Rasterizer.Options(includePartial = true, sampleType = PixelIsPoint)
    val cellType = DoubleConstantNoDataCellType
    val band: RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition] = fromKeyedFeature[Geometry](keyedFeatures, cellType, layoutDefinition, options)
    val datacube: RDD[(SpatialKey, MultibandTile)] = band.mapValues(tile => MultibandTile(tile))

    val metadata: TileLayerMetadata[SpatialKey] = TileLayerMetadata(
      cellType,
      layoutDefinition,
      extent,
      CRS.fromName(target_crs),
      KeyBounds(SpatialKey(0, 0), SpatialKey(layoutDefinition.layoutCols - 1, layoutDefinition.layoutRows - 1))
    )

    MultibandTileLayerRDD(datacube, metadata)
  }
}
