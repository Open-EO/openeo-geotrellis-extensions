package org.openeo.logging.py4j

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.nio.charset.StandardCharsets

/**
 * a ClientServer instance that can be instantiated in a process spawned by Py4J (~ py4j.GatewayServer#main())
 */
object ClientServer {
  def main(args: Array[String]): Unit = {
    val clientServer = new py4j.ClientServer.ClientServerBuilder().javaPort(0).build()
    clientServer.startServer()

    val listeningPort = clientServer.getJavaServer.getListeningPort
    println(listeningPort.toString)

    /* Exit on EOF or broken pipe.  This ensures that the server dies if its parent program dies. */
    try {
      val stdin = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))
      stdin.readLine
      System.exit(0)
    } catch {
      case _: IOException =>
        System.exit(1)
    }
  }
}
