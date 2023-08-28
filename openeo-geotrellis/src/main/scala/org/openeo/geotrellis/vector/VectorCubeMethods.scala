package org.openeo.geotrellis.vector

import io.circe.{Json, JsonObject}
import io.circe.parser
import geotrellis.layer.{KeyBounds, LayoutDefinition, Metadata, SpaceTimeKey, SpatialComponent, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{ArrayTile, CellSize, DoubleArrayFiller, DoubleConstantNoDataCellType, GridExtent, MultibandTile, NODATA, PixelIsPoint, RasterExtent, Tile}
import geotrellis.vector.{io, _}
import org.apache.spark.SparkContext
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.rasterize.Rasterizer.foreachCellByGeometry
import geotrellis.spark.rasterize.RasterizeRDD.fromKeyedFeature
import geotrellis.spark.{MultibandTileLayerRDD, withFeatureClipToGridMethods}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object VectorCubeMethods {

  def rasterizeWithDouble(geom: Geometry, rasterExtent: RasterExtent, value: Double): Tile =
    if(geom.isValid) {
      val cols = rasterExtent.cols
      val array = Array.ofDim[Double](rasterExtent.cols * rasterExtent.rows).fill(NODATA)
      val f2 = (col: Int, row: Int) => array(row * cols + col) = value
      foreachCellByGeometry(geom, rasterExtent)(f2)
      ArrayTile(array, rasterExtent.cols, rasterExtent.rows)
  } else throw new IllegalArgumentException("Cannot rasterize an invalid polygon")


  def vectorToRaster(path: String, datacube:Object): MultibandTileLayerRDD[SpatialKey] = {
    datacube match {
      case rdd1 if datacube.asInstanceOf[MultibandTileLayerRDD[SpatialKey]].metadata.bounds.get.maxKey.isInstanceOf[SpatialKey] =>
        vectorToRasterGeneric(path, rdd1.asInstanceOf[MultibandTileLayerRDD[SpatialKey]])
      case rdd2 if datacube.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]].metadata.bounds.get.maxKey.isInstanceOf[SpaceTimeKey] =>
        vectorToRasterGeneric(path, rdd2.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]])
      case _ => throw new IllegalArgumentException("Unsupported rdd type to vectorize: ${rdd}")
    }
  }

  /**
   * Convert a vector datacube to a raster datacube.
   *
   * @param path: Path to the geojson file.
   * @param targetDatacube: Target datacube to extract the crs and resolution from.
   * @return A raster datacube.
   */
  def vectorToRasterGeneric[K: SpatialComponent: ClassTag](path: String, targetDatacube: MultibandTileLayerRDD[K]): MultibandTileLayerRDD[SpatialKey] = {
    val sc = SparkContext.getOrCreate()
    val target_resolution = targetDatacube.metadata.layout.cellSize.resolution
    val target_crs: CRS = targetDatacube.metadata.crs

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
      val reprojected = geometry.reproject(src=CRS.fromEpsgCode(4326), dest=target_crs)
      Feature(reprojected, properties.head)
    })

    val featuresRDD: RDD[Feature[Geometry, Double]] = sc.parallelize(features)
    val extent = featuresRDD.map(_.geom.extent).reduce(_.combine(_))
    // TODO: Perhaps use layoutdefinition from target_datacube if provided.
    val layoutDefinition = LayoutDefinition(
      GridExtent[Long](extent, CellSize(target_resolution, target_resolution)),
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
      target_crs,
      KeyBounds(SpatialKey(0, 0), SpatialKey(layoutDefinition.layoutCols - 1, layoutDefinition.layoutRows - 1))
    )

    MultibandTileLayerRDD(datacube, metadata)
  }
}
