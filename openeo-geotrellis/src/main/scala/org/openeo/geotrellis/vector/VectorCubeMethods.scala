package org.openeo.geotrellis.vector

import io.circe.{Json, JsonObject}
import io.circe.parser
import geotrellis.layer.{KeyBounds, LayoutDefinition, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster
import geotrellis.raster.{ArrayTile, DoubleArrayFiller, DoubleConstantNoDataCellType, MultibandTile, NODATA, PixelIsPoint, RasterExtent, Tile}
import geotrellis.vector.{io, _}
import org.apache.spark.SparkContext
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.rasterize.Rasterizer.foreachCellByGeometry
import geotrellis.spark.rasterize.RasterizeRDD.fromKeyedFeature
import geotrellis.spark.{MultibandTileLayerRDD, withFeatureClipToGridMethods}
import org.apache.spark.rdd.RDD

import java.time.ZonedDateTime
import scala.collection.mutable

object VectorCubeMethods {

  def rasterizeWithDouble(geom: Geometry, rasterExtent: RasterExtent, value: Double): Tile =
    if(geom.isValid) {
      val cols = rasterExtent.cols
      val array = Array.ofDim[Double](rasterExtent.cols * rasterExtent.rows).fill(NODATA)
      val f2 = (col: Int, row: Int) => array(row * cols + col) = value
      foreachCellByGeometry(geom, rasterExtent)(f2)
      ArrayTile(array, rasterExtent.cols, rasterExtent.rows)
  } else throw new IllegalArgumentException("Cannot rasterize an invalid polygon")


  def vectorToRasterTemporal(path: String, datacube:Object): MultibandTileLayerRDD[SpaceTimeKey] = {
    datacube match {
      case rdd1 if datacube.asInstanceOf[MultibandTileLayerRDD[SpatialKey]].metadata.bounds.get.maxKey.isInstanceOf[SpatialKey] =>
        val rdd: MultibandTileLayerRDD[SpatialKey] = rdd1.asInstanceOf[MultibandTileLayerRDD[SpatialKey]]
        vectorToRasterTemporalInternal(path, rdd.metadata.layout, rdd.metadata.crs, rdd.metadata.bounds.get.toSpatial)
      case rdd2 if datacube.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]].metadata.bounds.get.maxKey.isInstanceOf[SpaceTimeKey] =>
        val rdd: MultibandTileLayerRDD[SpaceTimeKey] = rdd2.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]]
        vectorToRasterTemporalInternal(path, rdd.metadata.layout, rdd.metadata.crs, rdd.metadata.bounds.get.toSpatial)
      case _ => throw new IllegalArgumentException("Unsupported rdd type to vectorize: ${rdd}")
    }
  }

  def vectorToRasterSpatial(path: String, datacube:Object): MultibandTileLayerRDD[SpatialKey] = {
    datacube match {
      case rdd1 if datacube.asInstanceOf[MultibandTileLayerRDD[SpatialKey]].metadata.bounds.get.maxKey.isInstanceOf[SpatialKey] =>
        val rdd: MultibandTileLayerRDD[SpatialKey] = rdd1.asInstanceOf[MultibandTileLayerRDD[SpatialKey]]
        vectorToRasterSpatialInternal(path, rdd.metadata.layout, rdd.metadata.crs, rdd.metadata.bounds.get.toSpatial)
      case rdd2 if datacube.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]].metadata.bounds.get.maxKey.isInstanceOf[SpaceTimeKey] =>
        val rdd: MultibandTileLayerRDD[SpaceTimeKey] = rdd2.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]]
        vectorToRasterSpatialInternal(path, rdd.metadata.layout, rdd.metadata.crs, rdd.metadata.bounds.get.toSpatial)
      case _ => throw new IllegalArgumentException("Unsupported rdd type to vectorize: ${rdd}")
    }
  }

  private def isUTM(crs: CRS): Boolean = {
    crs.epsgCode.get.toString.startsWith("326") || crs.epsgCode.get.toString.startsWith("327")
  }

  def extractFeatures(path: String, target_crs: CRS, target_layout: LayoutDefinition): Map[String, Seq[(String, mutable.Buffer[Feature[Geometry, Double]])]] = {
    val source = scala.io.Source.fromFile(path)
    val sourceString = try source.mkString finally source.close()
    val json = parser.parse(sourceString).getOrElse(Json.Null)
    val listJson: List[Json] = json.hcursor.downField("features").as[List[Json]].getOrElse(throw new Exception("No features found in GeoJSON"))

    // Date -> [(bandName, Feature)]
    val featuresByDate = mutable.Map.empty[String, mutable.Map[String, mutable.Buffer[Feature[Geometry, Double]]]]
    val dateAndBandPattern = """(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)~(\w+)""".r
    val expectedBandOrder = mutable.Buffer.empty[String]
    listJson.foreach({ json: Json =>
      val geometry: Geometry = json.hcursor.downField("geometry").as[Geometry].getOrElse(throw new Exception("No geometry found in GeoJSON"))
      val reprojected: Geometry = geometry.reproject(src=CRS.fromEpsgCode(4326), dest=target_crs)
      val clipped: Geometry = reprojected.intersection(target_layout.extent)

      val propertiesMap: Seq[(String, Json)] = json.hcursor.downField("properties").as[JsonObject].getOrElse(JsonObject.empty).toList
      // Convert propertiesMap to date -> [(bandName, value)] mapping.
      // E.g. (2017-10-25T11:37:00Z~B1,1) => 2017-10-25T11:37:00Z -> (B1, 1)
      for ((key, jsonValue) <- propertiesMap) {
        val entry: (String, (String, Feature[Geometry, Double])) = key match {
          case dateAndBandPattern(date, band) =>
            date -> (band, Feature(clipped, jsonValue.as[Double].getOrElse(Double.NaN)))
          case _ =>
            "" -> (key, Feature(clipped, jsonValue.as[Double].getOrElse(Double.NaN)))
        }
        val (date, bandName, feature) = (entry._1, entry._2._1, entry._2._2)
        val emptyMap = mutable.Map.empty[String, mutable.Buffer[Feature[Geometry, Double]]]
        val bandsForDate = featuresByDate.getOrElse(date, emptyMap)
        bandsForDate.getOrElseUpdate(bandName, mutable.Buffer.empty[Feature[Geometry, Double]]).append(feature)
        if (!expectedBandOrder.contains(bandName)) {
          // We just take the order of the bands in the geojson file as the expected order.
          expectedBandOrder.append(bandName)
        }

        featuresByDate(entry._1) = bandsForDate
      }
    })
    // TODO: We can remove this copy if we assert that every geometry lists all bands, and in the same order.
    val featuresByDateOrdered: Map[String, Seq[(String, mutable.Buffer[Feature[Geometry, Double]])]] = featuresByDate.map({ case (date, bands) =>
      val orderedBands: Seq[(String, mutable.Buffer[Feature[Geometry, Double]])] = expectedBandOrder.map({ bandName =>
        bandName -> bands.getOrElse(bandName, mutable.Buffer.empty[Feature[Geometry, Double]])
      }).toList
      (date, orderedBands)
    }).toMap
    featuresByDateOrdered
  }

  private def featuresToBand(features: mutable.Buffer[Feature[Geometry, Double]], targetLayout: LayoutDefinition, cellType: raster.CellType): RDD[(SpatialKey, Tile)] = {
    val sc = SparkContext.getOrCreate()
    val options = Rasterizer.Options(includePartial = true, sampleType = PixelIsPoint)

    val featuresRDD: RDD[Feature[Geometry, Double]] = sc.parallelize(features)
    val keyedFeatures: RDD[(SpatialKey, Feature[Geometry, Double])] = featuresRDD.clipToGrid(targetLayout)
    val band: RDD[(SpatialKey, Tile)] = fromKeyedFeature[Geometry](keyedFeatures, cellType, targetLayout, options)
    band
  }

  /**
   * Convert a vector datacube to a raster datacube without a time dimension.
   *
   * @param path: Path to the geojson file.
   * @param targetDatacube: Target datacube to extract the crs and resolution from.
   * @return A raster datacube.
   */
  private def vectorToRasterSpatialInternal(path: String, targetLayout: LayoutDefinition, targetCRS: CRS, spatialBounds: KeyBounds[SpatialKey]): MultibandTileLayerRDD[SpatialKey] = {
    val cellType = DoubleConstantNoDataCellType

    val featureBands: Seq[(String, mutable.Buffer[Feature[Geometry, Double]])] = extractFeatures(path, targetCRS, targetLayout)("")
    val bands: RDD[(SpatialKey, MultibandTile)] = featureBands.map { case (_bandName, features) =>
      featuresToBand(features, targetLayout, cellType)
    }.reduce{_ union _}.groupByKey().mapValues(tiles => MultibandTile(tiles))

    val metadata: TileLayerMetadata[SpatialKey] = TileLayerMetadata(
      cellType,
      targetLayout,
      targetLayout.extent,
      targetCRS,
      spatialBounds
    )

    MultibandTileLayerRDD(bands, metadata)
  }

  /**
   * Convert a vector datacube to a raster datacube with a time dimension.
   *
   * @param path: Path to the geojson file.
   * @param t
   * @return A raster datacube.
   */
  private def vectorToRasterTemporalInternal(path: String, targetLayout: LayoutDefinition, targetCRS: CRS, spatialBounds: KeyBounds[SpatialKey]): MultibandTileLayerRDD[SpaceTimeKey] = {
    val cellType = DoubleConstantNoDataCellType

    // Create the datacube.
    val features: Map[String, Seq[(String, mutable.Buffer[Feature[Geometry, Double]])]] = extractFeatures(path, targetCRS, targetLayout)
    val datacube: RDD[(SpaceTimeKey, MultibandTile)] = features.map { case (date, featureBands) =>
      val bands: RDD[(SpatialKey, MultibandTile)] = featureBands.map { case (_bandName, features) =>
        featuresToBand(features, targetLayout, cellType)
      }.reduce{_ union _}.groupByKey().mapValues(tiles => MultibandTile(tiles))
      bands.map(t => (SpaceTimeKey(t._1, TemporalKey(ZonedDateTime.parse(date))), t._2))
    }.reduce(_ union _)

    // Create the metadata.
    val dates: Iterable[ZonedDateTime] = features.keys.map{ ds => ZonedDateTime.parse(ds)}
    var minDate: ZonedDateTime = dates.head
    var maxDate: ZonedDateTime = dates.head
    for (date <- dates) {
      if (date.isBefore(minDate)) {
        minDate = date
      } else if (date.isAfter(maxDate)) {
        maxDate = date
      }
    }
    val minKey = SpaceTimeKey(spatialBounds.minKey, TemporalKey(minDate))
    val maxKey = SpaceTimeKey(spatialBounds.maxKey, TemporalKey(maxDate))
    val timeBounds: KeyBounds[SpaceTimeKey] = new KeyBounds[SpaceTimeKey](minKey, maxKey)
    val metadata: TileLayerMetadata[SpaceTimeKey] = TileLayerMetadata(
      cellType,
      targetLayout,
      targetLayout.extent,
      targetCRS,
      timeBounds
    )

    MultibandTileLayerRDD(datacube, metadata)
  }
}
