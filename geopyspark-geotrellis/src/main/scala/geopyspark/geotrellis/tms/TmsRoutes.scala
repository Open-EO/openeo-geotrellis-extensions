package geopyspark.geotrellis.tms

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.{Marshaller, ToResponseMarshaller}
import akka.http.scaladsl.model.MediaTypes.`image/png`
import akka.http.scaladsl.model.{ContentType, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.{Directives, Route}
import cats.implicits._
import geopyspark.geotrellis._
import geotrellis.raster._
import org.apache.log4j.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters._


trait TMSServerRoute extends Directives with AkkaSystem.LoggerExecutor {
  val logger = Logger.getLogger(this.getClass)

  def startup(): Unit = {}
  def shutdown(): Unit = {}

  def root: Route
  def route(server: TMSServer): Route = {
    get { root ~ path("handshake") { complete { server.handshake } } }
  }

  def time[T](msg: String)(f: => T) = {
    val start = System.currentTimeMillis
    val v = f
    val end = System.currentTimeMillis
    logger.info(s"[TIMING] $msg: ${java.text.NumberFormat.getIntegerInstance.format(end - start)} ms")
    v
  }

  implicit def pngMarshaller: ToResponseMarshaller[Array[Byte]] = Marshaller.oneOf(
    Marshaller.withFixedContentType(ContentType(`image/png`)) { img =>
      HttpResponse(entity = HttpEntity(ContentType(`image/png`), img))
    })
}

object TMSServerRoutes {

  private class RenderingTileRoute(reader: TileReader, renderer: TileRender) extends TMSServerRoute {
    def root: Route =
      pathPrefix("tile" / IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
        val tileFuture =
          reader
            .retrieve(zoom, x, y)
            .map(_.map{tile =>
              if (renderer.requiresEncoding()) {
                renderer.renderEncoded(geopyspark.util.PythonTranslator.toPython(tile))
              } else {
                renderer.render(tile)
              }
            })
        onSuccess(tileFuture) {
          case Some(t) => complete(t)
          case None => complete(204, None)
        }
      }

    override def startup() = reader.startup()
    override def shutdown() = reader.shutdown()
  }

  private class CompositingTileRoute(readers: List[TileReader], compositer: TileCompositer) extends TMSServerRoute {
    def root: Route =
      pathPrefix("tile" / IntNumber / IntNumber / IntNumber) { (zoom, x, y) =>
        val tileFutures: List[Future[Option[MultibandTile]]] = readers.map(_.retrieve(zoom, x, y))
        val futureTiles: Future[Option[Array[MultibandTile]]] = tileFutures.sequence.map(_.sequence).map(_.map(_.toArray))
        val composited: Future[Option[Array[Byte]]] =
          futureTiles
            .map(
              _.map(array =>
                if (compositer.requiresEncoding()) {
                  compositer.compositeEncoded(array.map{tile => geopyspark.util.PythonTranslator.toPython(tile)})
                } else {
                  compositer.composite(array)
                }
              )
            )

        onSuccess(composited) {
          case Some(img) => complete(img)
          case None => complete(204, None)
        }
      }

    override def startup() = readers.foreach(_.startup())
    override def shutdown() = readers.foreach(_.shutdown())
  }

  def renderingTileRoute(reader: TileReader, renderer: TileRender): TMSServerRoute = new RenderingTileRoute(reader, renderer)

  def compositingTileRoute(readers: java.util.ArrayList[TileReader], compositer: TileCompositer): TMSServerRoute = new CompositingTileRoute(readers.asScala.toList, compositer)

}
