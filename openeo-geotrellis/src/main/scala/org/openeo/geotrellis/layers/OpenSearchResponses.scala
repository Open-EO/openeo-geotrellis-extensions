package org.openeo.geotrellis.layers

import java.net.URI
import java.time.ZonedDateTime

import _root_.io.circe.parser.decode
import cats.syntax.either._
import cats.syntax.show._
import io.circe.generic.auto._
import io.circe.{Decoder, HCursor, Json}
import geotrellis.vector._

object OpenSearchResponses {
  implicit val decodeUrl: Decoder[URI] = Decoder.decodeString.map(URI.create(_))
  implicit val decodeDate: Decoder[ZonedDateTime] = Decoder.decodeString.map{s:CharSequence => ZonedDateTime.parse(  s.toString().split('/')(0))}

  case class Link(href: URI, title: Option[String])
  case class Feature(id: String, bbox: Extent, nominalDate: ZonedDateTime, links: Array[Link], resolution: Option[Int])
  case class FeatureCollection(itemsPerPage: Int, features: Array[Feature])

  object FeatureCollection {
    def parse(json: String): FeatureCollection = {
      implicit val decodeFeature: Decoder[Feature] = new Decoder[Feature] {
        override def apply(c: HCursor): Decoder.Result[Feature] = {
          for {
            id <- c.downField("id").as[String]
            bbox <- c.downField("bbox").as[Array[Double]]
            nominalDate <- c.downField("properties").downField("date").as[ZonedDateTime]
            links <- c.downField("properties").downField("links").as[Map[String, Array[Link]]]
            resolution = c.downField("properties").downField("productInformation").downField("resolution").downArray.first.as[Int].toOption
          } yield {
            val Array(xMin, yMin, xMax, yMax) = bbox
            val extent = Extent(xMin, yMin, xMax, yMax)

            Feature(id, extent, nominalDate, links.values.flatten.toArray, resolution)
          }
        }
      }

      decode[FeatureCollection](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }

  object STACFeatureCollection {
    def parse(json: String, toS3URL:Boolean=true): FeatureCollection = {
      implicit val decodeFeature: Decoder[Feature] = new Decoder[Feature] {
        override def apply(c: HCursor): Decoder.Result[Feature] = {
          for {
            id <- c.downField("id").as[String]
            bbox <- c.downField("bbox").as[Array[Double]]
            nominalDate <- c.downField("properties").downField("datetime").as[ZonedDateTime]
            links <- c.downField("assets").as[Map[String, Link]]
            resolution = c.downField("properties").downField("gsd").as[Int].toOption
          } yield {
            val Array(xMin, yMin, xMax, yMax) = bbox
            val extent = Extent(xMin, yMin, xMax, yMax)

            val harmonizedLinks = links.map { t =>
              val href = t._2.href
              if(toS3URL){
                val bucket = href.getHost.split('.')(0)
                val s3href = URI.create("s3://" + bucket +href.getPath)
                Link(s3href, Some(t._1))
              }
              else{
                Link(href, Some(t._1)) }
              }
            Feature(id, extent, nominalDate, harmonizedLinks.toArray, resolution)
          }
        }
      }

      implicit val decodeFeatureCollection: Decoder[FeatureCollection] = new Decoder[FeatureCollection] {
        override def apply(c: HCursor): Decoder.Result[FeatureCollection] = {
          for {
            itemsPerPage <- c.downField("numberReturned").as[Int]
            features <- c.downField("features").as[Array[Feature]]
          } yield {
            FeatureCollection(itemsPerPage, features)
          }
        }
      }

      decode[FeatureCollection](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }

  object CreoFeatureCollection {
    def parse(json: String): FeatureCollection = {
      implicit val decodeFeature: Decoder[Feature] = new Decoder[Feature] {
        override def apply(c: HCursor): Decoder.Result[Feature] = {
          for {
            id <- c.downField("properties").downField("productIdentifier").as[String]
            geometry <- c.downField("geometry").as[Json]
            nominalDate <- c.downField("properties").downField("startDate").as[ZonedDateTime]
            links <- c.downField("properties").downField("links").as[Array[Link]]
            resolution = c.downField("properties").downField("resolution").as[Int].toOption
          } yield {
            val extent = geometry.toString().parseGeoJson[Geometry].extent

            Feature(id, extent, nominalDate, links, resolution)
          }
        }
      }

      implicit val decodeFeatureCollection: Decoder[FeatureCollection] = new Decoder[FeatureCollection] {
        override def apply(c: HCursor): Decoder.Result[FeatureCollection] = {
          for {
            itemsPerPage <- c.downField("properties").downField("itemsPerPage").as[Int]
            features <- c.downField("features").as[Array[Feature]]
          } yield {
            FeatureCollection(itemsPerPage, features)
          }
        }
      }

      decode[FeatureCollection](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }

  case class CreoCollection(name: String)
  case class CreoCollections(collections: Array[CreoCollection])

  object CreoCollections {
    def parse(json: String): CreoCollections = {
      decode[CreoCollections](json)
      .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }
}
