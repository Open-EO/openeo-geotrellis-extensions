package org.openeo.geotrelliscommon

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.net.URLClassLoader
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.jar.{JarEntry, JarOutputStream}

class ClasspathResourceTest {
  @TempDir
  var tempDir: Path = _

  @Test
  def returnsFileResourceWithoutCopying(): Unit = {
    val resource = tempDir.resolve("grid.tif")
    Files.write(resource, Array[Byte](1, 2, 3))
    val loader = new URLClassLoader(Array(tempDir.toUri.toURL), null)
    try {
      assertEquals(resource.toUri, resolveClasspathResource("grid.tif", loader))
    } finally loader.close()
  }

  @Test
  def extractsJarResourcesWithOriginalNamesAndExtensions(): Unit = {
    val jar = tempDir.resolve("resources.jar")
    val out = new JarOutputStream(Files.newOutputStream(jar))
    val names = Seq("org/openeo/egm96.tif", "data/a.nc", "data/README", "data/.hidden")
    try names.foreach { name =>
      out.putNextEntry(new JarEntry(name))
      out.write(name.getBytes(UTF_8))
      out.closeEntry()
    } finally out.close()

    val loader = new URLClassLoader(Array(jar.toUri.toURL), null)
    try names.foreach { name =>
      val resolved = resolveClasspathResource(name, loader)
      val path = Paths.get(resolved)
      val original = name.substring(name.lastIndexOf('/') + 1)
      val dotIndex = original.lastIndexOf('.')
      val stem = if (dotIndex > 0) original.substring(0, dotIndex) else original
      val suffix = if (dotIndex > 0) original.substring(dotIndex) else ""
      try {
        assertEquals("file", resolved.getScheme)
        assertTrue(path.getFileName.toString.startsWith(stem + "-"))
        assertTrue(path.getFileName.toString.endsWith(suffix))
        if (suffix.isEmpty && !original.startsWith(".")) {
          assertFalse(path.getFileName.toString.contains("."))
        }
        assertEquals(name, new String(Files.readAllBytes(path), UTF_8))
        assertEquals(resolved, resolveClasspathResource(name, loader))
      } finally Files.deleteIfExists(path)
    } finally loader.close()
  }

  @Test
  def missingResourceFailsExplicitly(): Unit = {
    val loader = new URLClassLoader(Array(tempDir.toUri.toURL), null)
    try {
      val error = assertThrows(classOf[IllegalArgumentException],
        () => resolveClasspathResource("missing.tif", loader))
      assertTrue(error.getMessage.contains("missing.tif"))
    } finally loader.close()
  }
}
