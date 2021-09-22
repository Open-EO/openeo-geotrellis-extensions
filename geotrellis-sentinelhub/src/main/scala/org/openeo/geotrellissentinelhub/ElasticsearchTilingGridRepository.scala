package org.openeo.geotrellissentinelhub

import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, Hit, HitReader}
import geotrellis.vector._
import _root_.io.circe.parser.parse
import cats.syntax.either._
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.http.JavaClient

import scala.util.{Failure, Success, Try}

object ElasticsearchTilingGridRepository {
  case class GridTile(_id: String, location: Geometry)

  private implicit object GridTileHitReader extends HitReader[GridTile] {
    override def read(hit: Hit): Try[GridTile] = {
      val tileId = hit.id

      val location = parse(hit.sourceAsString)
        .flatMap(source => source.hcursor.downField("location").as[MultiPolygon])

      location match {
        case Right(location) => Success(GridTile(tileId, location))
        case Left(e) => Failure(e)
      }
    }
  }
}

// TODO: rename to ElasticsearchTilingGridRepository
class ElasticsearchTilingGridRepository(elasticsearchUri: String) {
  import ElasticsearchTilingGridRepository._

  private def elasticClient: ElasticClient = ElasticClient(JavaClient(ElasticProperties(elasticsearchUri)))

  def intersectingGridTiles(tilingGridIndex: String, geometry: Geometry): Iterable[GridTile] = {
    val client = elasticClient

    try {
      val resp = client.execute {
        search(tilingGridIndex)
          .rawQuery(
            s"""
               |{
               |  "geo_shape": {
               |    "location": {
               |      "shape": ${geometry.toGeoJson()},
               |      "relation": "intersects"
               |    }
               |  }
               |}""".stripMargin)
          .size(10000)
      }.await

      resp.result
        .safeTo[GridTile]
        .map(_.get)
    } finally client.close()
  }

  def getGeometry(tilingGridIndex: String, tileId: String): Geometry = {
    val client = elasticClient

    try {
      val resp = client.execute {
        get(tileId).from(tilingGridIndex)
      }.await

      resp.result
        .safeTo[GridTile]
        .map(_.location)
        .get
    } finally client.close()
  }
}
