package org.openeo.geotrelliscommon

import org.junit.Test

import scala.sys.process._

class DockerTest {

  @Test
  def testLs(): Unit = {
    val cmd = "ls -al"
    val output = cmd.!!
    println(output)
  }

  @Test
  def testPs(): Unit = {
    val cmd = "docker ps"
    val output = cmd.!!
    println(output)
  }

  @Test
  def testPull(): Unit = {
    val cmd = "docker pull hello-world"
    val output = cmd.!!
    println(output)
  }

  @Test
  def testRun(): Unit = {
    val cmd = "docker run hello-world"
    val output = cmd.!!
    println(output)
  }
}
