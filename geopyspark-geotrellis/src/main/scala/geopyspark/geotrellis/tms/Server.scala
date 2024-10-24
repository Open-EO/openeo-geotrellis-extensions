package geopyspark.geotrellis.tms

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.ActorMaterializer

import java.net.InetAddress
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

object AkkaSystem {
  implicit val system = ActorSystem("geopyspark-tile-server")
  implicit val materializer = ActorMaterializer()

  trait LoggerExecutor {
    protected implicit val log = Logging(system, "tms")
  }
}

class TMSServer(router: TMSServerRoute) {
  import AkkaSystem._

  var _handshake = ""
  var binding: ServerBinding = null

  def localHost(): String = InetAddress.getLocalHost().getHostName()
  def host(): String = binding.localAddress.getHostName()
  def port(): Int = binding.localAddress.getPort()
  def unbind(): Unit = {
    Await.ready(binding.unbind, 10.seconds)
    binding = null
    router.shutdown
  }

  def bind(host: String): ServerBinding = {
    router.startup

    var futureBinding: scala.util.Try[Future[ServerBinding]] = null
    do {
      var portReq = scala.util.Random.nextInt(16383) + 49152
      futureBinding = scala.util.Try(Http()(system).bindAndHandle(router.route(this) , host, portReq))
    } while (futureBinding.isFailure)
    binding = Await.result(futureBinding.get, 10.seconds)
    binding
  }

  def bind(host: String, requestedPort: Int): ServerBinding = {
    router.startup

    val futureBinding = Http()(system).bindAndHandle(router.route(this) , host, requestedPort)
    binding = Await.result(futureBinding, 10.seconds)
    binding
  }

  def setHandshake(str: String) = { _handshake = str }
  def handshake(): String = _handshake
}

object TMSServer {
  def createServer(route: TMSServerRoute) = new TMSServer(route)
}
