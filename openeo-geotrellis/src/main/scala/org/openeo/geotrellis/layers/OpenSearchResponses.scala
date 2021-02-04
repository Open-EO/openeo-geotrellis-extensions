package org.openeo.geotrellis.layers

import java.io.FileInputStream
import java.lang.System.getenv
import java.net.URI
import java.nio.file.Paths
import java.time.ZonedDateTime

import _root_.io.circe.parser.decode
import cats.syntax.either._
import cats.syntax.show._
import io.circe.generic.auto._
import io.circe.{Decoder, HCursor, Json}
import geotrellis.vector._
import javax.net.ssl.HttpsURLConnection

import scala.xml.{Node, XML}

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


    private def getAwsDirect() = {
      "TRUE".equals(getenv("AWS_DIRECT"))
    }

    private def getFilePathsFromManifest(path: String) = {
      var gdalPrefix = ""

      val inputStream = if (path.startsWith("https://")) {
        gdalPrefix = "/vsicurl"

        val uri = new URI(path)
        uri.resolve(s"${uri.getPath}/manifest.safe").toURL
          .openConnection.asInstanceOf[HttpsURLConnection]
          .getInputStream
      } else {
        gdalPrefix = if (getAwsDirect()) "/vsis3" else ""

        if(path.startsWith("/eodata")) {
          //reading from /eodata is extremely slow
          val url = path.replace("/eodata","https://finder.creodias.eu/files")
          val uri = new URI(url)
          uri.resolve(s"${uri.getPath}/manifest.safe").toURL
            .openConnection.asInstanceOf[HttpsURLConnection]
            .getInputStream
        }else{
          new FileInputStream(Paths.get(path, "manifest.safe").toFile)
        }
      }

      val xml = XML.load(inputStream)


      (xml \\ "dataObject" )
        .map((dataObject: Node) =>{
          val title = dataObject \\ "@ID"
          val fileLocation = dataObject \\ "fileLocation" \\ "@href"
          Link(URI.create(s"$gdalPrefix${if (path.startsWith("/")) "" else "/"}$path" + s"/${Paths.get(fileLocation.toString).normalize().toString}"),Some(title.toString))
        })
    }

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

            if(id.endsWith(".SAFE")){
              val all_links = getFilePathsFromManifest(id)
              Feature(id, extent, nominalDate, all_links.toArray, resolution)
            }else{
              Feature(id, extent, nominalDate, links, resolution)
            }

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
