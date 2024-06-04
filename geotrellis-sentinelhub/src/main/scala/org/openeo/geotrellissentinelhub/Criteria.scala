package org.openeo.geotrellissentinelhub

import java.util
import scala.collection.JavaConverters._

object Criteria {
  private def isPropagated(metadataProperty: String): Boolean =
    !Set("provider:backend", "federation:backends", "tileId").contains(metadataProperty)

  def toQueryProperties(metadata_properties: util.Map[String, util.Map[String, Any]],
                        collectionId: String): util.Map[String, util.Map[String, Any]] = {
    val queryProperties = for {
      (metadataProperty, criteria) <- metadata_properties.asScala if isPropagated(metadataProperty)
    } yield toQueryPropertyName(metadataProperty, collectionId) -> toQueryCriteria(criteria)

    queryProperties.get("eo:cloud_cover") match {
      case Some(criterion) =>
        if (criterion.size() > 1 || !criterion.containsKey("lte"))
          throw new IllegalArgumentException(s"query property eo:cloud_cover only supports operator lte")
      case _ =>
    }

    queryProperties.asJava
  }

  private def toQueryPropertyName(metadataPropertyName: String, collectionId: String): String =
    (metadataPropertyName, collectionId) match {
      case ("orbit_id", _) => "sat:absolute_orbit"
      case ("orbitDirection", _) => "sat:orbit_state"
      case ("polarization", "sentinel-1-grd") => "s1:polarization"
      case ("resolution", "sentinel-1-grd") => "s1:resolution"
      case ("sar:polarization", "sentinel-1-grd") => "s1:polarization"
      case ("timeliness", "sentinel-1-grd") => "s1:timeliness"
      case ("timeliness", "sentinel-5p-l2") => "s5p:timeliness"
      case ("type", "sentinel-5p-l2") => "s5p:type"
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
      (metadataProperty, criteria) <- metadata_properties.asScala if isPropagated(metadataProperty)
      (operator, value) <- criteria.asScala
    } yield (metadataProperty, operator, value)

    def abort(metadataProperty: String, operator: String, value: Any): Nothing =
      throw new IllegalArgumentException(s"unsupported dataFilter $metadataProperty $operator $value")

    flattenedCriteria
      .map {
        case ("eo:cloud_cover", "lte", value) => "maxCloudCoverage" -> value
        case (metadataProperty @ "eo:cloud_cover", operator, value) => abort(metadataProperty, operator, value)
        case ("sat:orbit_state", "eq", value) => "orbitDirection" -> value
        case ("sar:polarization", "eq", value) => "polarization" -> value
        case ("sar:instrument_mode", "eq", value) => "acquisitionMode" -> value
        case (metadataProperty, "eq", value) => metadataProperty -> value
        case (metadataProperty, operator, value) => abort(metadataProperty, operator, value)
      }
      .toMap.asJava
  }
}
