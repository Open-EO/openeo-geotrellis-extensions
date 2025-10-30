package org.openeo.geotrellis.layers

import io.circe.Json
import io.circe.syntax._
import geotrellis.vector._
import org.openeo.geotrelliscommon.AuxiliaryFileWriter
import org.openeo.opensearch.OpenSearchResponses.Feature
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import java.nio.file.{Files, Path, Paths}


object DerivedFromDocumentWriter {
  private val logger: Logger = LoggerFactory.getLogger(classOf[DerivedFromDocumentWriter])
}

class DerivedFromDocumentWriter(inputFeatures: Seq[Feature]) extends AuxiliaryFileWriter {
  import DerivedFromDocumentWriter._

  override def write(jobId: Option[String]): Path = {
    val workDir = Paths.get("").toAbsolutePath // analogous to https://jira.vito.be/browse/EP-3385
    val derivedFromDocument = Files.createTempFile(workDir, s"${jobId.getOrElse("unknown-job")}_input_items_", ".geojson")

    writeDerivedFromDocument(derivedFromDocument, inputFeatures)
    logger.debug(s"wrote ${inputFeatures.size} input STAC item(s) to $derivedFromDocument")
    derivedFromDocument
  }

  private def writeDerivedFromDocument(targetFile: Path, inputFeatures: Seq[Feature]): Unit = {
    def asDerivedFromFeature(inputFeature: Feature): Map[String, Json] = {
      def asDerivedFromLink(selfUrl: URI): Map[String, String] = Map(
        "rel" -> "derived_from",
        "href" -> selfUrl.toString,
        "title" -> inputFeature.id,
        // TODO: add media type?
      )

      Map(
        "type" -> "Feature".asJson,
        "stac_version" -> "1.1.0".asJson,
        "id" -> inputFeature.id.asJson,
        "geometry" -> inputFeature.geometry.getOrElse(inputFeature.bbox.toPolygon()).asJson,
        "bbox" -> Seq(
          inputFeature.bbox.xmin, inputFeature.bbox.ymin,
          inputFeature.bbox.xmax, inputFeature.bbox.ymax
        ).asJson,
        "properties" -> Map[String, Json]().asJson,
        "links" -> inputFeature.selfUrl.map(selfUrl => Seq(asDerivedFromLink(selfUrl))).getOrElse(Seq()).asJson,
        "assets" -> Map[String, Json]().asJson, // TODO: for load_stac
      )
    }

    val derivedFromDocument = Map(
      "type" -> "FeatureCollection".asJson,
      "features" -> inputFeatures.map(asDerivedFromFeature).asJson,
    ).asJson

    Files.write(targetFile, derivedFromDocument.noSpaces.getBytes("UTF-8"))
  }
}
