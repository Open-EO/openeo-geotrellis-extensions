package org.openeo.geotrellissentinelhub

import org.slf4j.LoggerFactory
import java.util
import scala.collection.JavaConverters._

object Criteria {
  private val logger = LoggerFactory.getLogger(Criteria.getClass)

  def toQueryProperties(metadata_properties: util.Map[String, util.Map[String, Any]]): util.Map[String, util.Map[String, Any]] = {
    val queryProperties = for {
      (metadataProperty, criteria) <- metadata_properties.asScala if metadataProperty != "provider:backend"
    } yield toQueryPropertyName(metadataProperty) -> toQueryCriteria(criteria)

    queryProperties.get("eo:cloud_cover") match {
      case Some(criterion) =>
        if (criterion.size() > 1 || !criterion.containsKey("lte"))
          throw new IllegalArgumentException(s"query property eo:cloud_cover only supports operator lte")
      case _ =>
    }

    queryProperties.asJava
  }

  private def toQueryPropertyName(metadataPropertyName: String): String = metadataPropertyName match {
    case "orbitDirection" => "sat:orbit_state"
    case "sar:polarization" => "polarization"
    case _ => metadataPropertyName
  }

  private def toQueryCriteria(criteria: util.Map[String, Any]): util.Map[String, Any] = {
    val queryCriteria = criteria.asScala

    val actualQueryOperators = queryCriteria.keySet
    val supportedQueryOperators = Set("eq", "lte")
    val unsupportedQueryOperators = actualQueryOperators diff supportedQueryOperators

    if (unsupportedQueryOperators.nonEmpty) {
      throw new IllegalArgumentException(s"unsupported query operators $unsupportedQueryOperators")
    }

    queryCriteria.asJava
  }

  def toDataFilters(metadata_properties: util.Map[String, util.Map[String, Any]], band_names: Seq[String]): util.Map[String, Any] = {
    val flattenedCriteria = for {
      (metadataProperty, criteria) <- metadata_properties.asScala if metadataProperty != "provider:backend"
      (operator, value) <- criteria.asScala
    } yield (metadataProperty, operator, value)

    def abort(metadataProperty: String, operator: String, value: Any): Nothing =
      throw new IllegalArgumentException(s"unsupported dataFilter $metadataProperty $operator $value")

    var filtersDict = flattenedCriteria
      .map {
        case ("eo:cloud_cover", "lte", value) => "maxCloudCoverage" -> value
        case (metadataProperty @ "eo:cloud_cover", operator, value) => abort(metadataProperty, operator, value)
        case ("sat:orbit_state", "eq", value) => "orbitDirection" -> value
        case ("sar:polarization", "eq", value) => "polarization" -> value
        case ("sar:instrument_mode", "eq", value) => "acquisitionMode" -> value
        case (metadataProperty, "eq", value) => metadataProperty -> value
        case (metadataProperty, operator, value) => abort(metadataProperty, operator, value)
      }
      .toMap
    if (!filtersDict.contains("polarization")) {
      // https://docs.sentinel-hub.com/api/latest/data/sentinel-1-grd/#available-bands-and-data
      val bn = band_names.toSet
      val polarization: Option[String] = bn match {
        case _ if bn.contains("HH") && bn.contains("HV") => Some("DH")
        case _ if bn.contains("VV") && bn.contains("VH") => Some("DV")
        case _ if bn.contains("HV") => Some("HV")
        case _ if bn.contains("VH") => Some("VH")
        case _ => None
      }
      for {
        p <- polarization
      } {
        logger.info("No polarization was specified, using one based on band selection: " + p)
        filtersDict = filtersDict + ("polarization" -> p)
      }
    }
    filtersDict.asJava
  }
}
