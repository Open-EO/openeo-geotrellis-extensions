package org.openeo.geotrellissentinelhub

import java.util
import scala.collection.JavaConverters._

object Criteria {
  def toQueryProperties(metadata_properties: util.Map[String, util.Map[String, Any]]): util.Map[String, util.Map[String, Any]] = {
    val queryProperties = for {
      (metadataProperty, criteria) <- metadata_properties.asScala
    } yield toQueryPropertyName(metadataProperty) -> toQueryCriteria(criteria)

    queryProperties.asJava
  }

  private def toQueryPropertyName(metadataPropertyName: String): String = metadataPropertyName match {
    case "orbitDirection" => "sat:orbit_state"
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

  def toDataFilters(metadata_properties: util.Map[String, util.Map[String, Any]]): util.Map[String, Any] = {
    val flattenedCriteria = for {
      (metadataProperty, criteria) <- metadata_properties.asScala
      (operator, value) <- criteria.asScala
    } yield (metadataProperty, operator, value)

    flattenedCriteria
      .map {
        case ("eo:cloud_cover", "lte", value) => "maxCloudCoverage" -> value
        case ("sat:orbit_state", "eq", value) => "orbitDirection" -> value
        case (metadataProperty, "eq", value) => metadataProperty -> value
        case (metaDataProperty, operator, value) =>
          throw new IllegalArgumentException(s"unsupported dataFilter $metaDataProperty $operator $value")
      }
      .toMap.asJava
  }
}
