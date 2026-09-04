package org.openeo.sar.stac

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.openeo.sar.io.UriIO

import java.net.URI
import scala.jdk.CollectionConverters._

/** Pointer to the resources that make up one S1 GRD scene, resolved
 *  from a STAC item (CDSE or compatible).
 *
 *  CDSE exposes the SAFE as an object-store prefix with assets whose hrefs
 *  point to the individual files. The asset roles / keys vary between
 *  providers, so this loader is intentionally permissive: it finds assets
 *  by SAFE-relative path conventions. */
final case class StacAssets(
  perPol: Map[String, PolarisationAssets],   // key: lowercase polarisation ("vv","vh",...)
  /** Bounding box from the STAC item (lon_min, lat_min, lon_max, lat_max). */
  bboxWgs84: (Double, Double, Double, Double)
)

final case class PolarisationAssets(
  measurement: URI,
  productAnnotation: URI,
  calibration: URI,
  noise: URI
)

object StacItemLoader {
  private val mapper: ObjectMapper = {
    val m = new ObjectMapper(); m.registerModule(DefaultScalaModule); m
  }

  def load(itemUri: URI): StacAssets = {
    val in = UriIO.openInputStream(itemUri)
    val root =
      try mapper.readTree(in)
      finally in.close()
    val assets = root.get("assets")
    require(assets != null, "STAC item has no assets")

    val hrefs: Map[String, URI] = assets.fields().asScala.flatMap { e =>
      Option(e.getValue.get("href")).map(h => e.getKey -> URI.create(h.asText()))
    }.toMap

    // Group by polarisation by looking at SAFE-relative path conventions:
    //   measurement/s1*-grd-{pol}-*.tiff
    //   annotation/s1*-grd-{pol}-*.xml
    //   annotation/calibration/calibration-s1*-grd-{pol}-*.xml
    //   annotation/calibration/noise-s1*-grd-{pol}-*.xml
    val polRegex = "(?i)-grd-(vv|vh|hh|hv)-".r

    def polOf(u: URI): Option[String] = polRegex.findFirstMatchIn(u.toString).map(_.group(1).toLowerCase)

    val pols = hrefs.values.flatMap(polOf).toSet
    val perPol = pols.map { pol =>
      val forPol = hrefs.values.filter(u => polOf(u).contains(pol)).toSeq
      def find(predicate: URI => Boolean, what: String): URI =
        forPol.find(predicate).getOrElse(
          throw new IllegalStateException(s"missing $what asset for polarisation $pol"))
      val measurement = find(u => u.getPath.contains("/measurement/") && u.getPath.endsWith(".tiff"), "measurement")
      val annot       = find(u => u.getPath.contains("/annotation/") &&
                                  !u.getPath.contains("/calibration/") &&
                                  u.getPath.endsWith(".xml"), "annotation")
      val calib       = find(u => u.getPath.contains("/annotation/calibration/") &&
                                  u.getPath.contains("calibration-") &&
                                  u.getPath.endsWith(".xml"), "calibration")
      val noise       = find(u => u.getPath.contains("/annotation/calibration/") &&
                                  u.getPath.contains("noise-") &&
                                  u.getPath.endsWith(".xml"), "noise")
      pol -> PolarisationAssets(measurement, annot, calib, noise)
    }.toMap

    val bboxNode: JsonNode = root.get("bbox")
    require(bboxNode != null && bboxNode.size() == 4, "STAC item missing bbox")
    val bbox = (bboxNode.get(0).asDouble(), bboxNode.get(1).asDouble(),
                bboxNode.get(2).asDouble(), bboxNode.get(3).asDouble())

    StacAssets(perPol, bbox)
  }
}
