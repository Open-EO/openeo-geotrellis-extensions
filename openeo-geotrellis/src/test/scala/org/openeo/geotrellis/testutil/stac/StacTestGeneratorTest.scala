package org.openeo.geotrellis.testutil.stac

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeAll, Test}
import org.openeo.geotrellis.testutil.stac.StandardStacTestCollections._

import java.nio.file.Files

object StacTestGeneratorTest {
  private var collection: GeneratedCollection = _

  @BeforeAll
  def generate(): Unit =
    collection = StacTestGenerator.ensureGenerated(fileLayerProviderCollection)
}

class StacTestGeneratorTest {
  import StacTestGeneratorTest._

  @Test
  def filesAreCreated(): Unit = {
    val spec = fileLayerProviderCollection
    for (item <- spec.items) {
      for (asset <- item.assets) {
        val path = collection.assetPath(item.id, asset.fileName)
        assertTrue(Files.exists(path), s"Missing: $path")
        assertTrue(Files.size(path) > 0, s"Empty: $path")
      }
      assertTrue(Files.exists(collection.itemDir(item.id).resolve("item.json")))
    }
    assertTrue(Files.exists(collection.collectionJsonPath))
  }

  @Test
  def hashFilePreventRegeneration(): Unit = {
    val outputDir = collection.outputDir
    val hashFile  = outputDir.resolve(".collection_hash")
    assertTrue(Files.exists(hashFile))
    val mtime = Files.getLastModifiedTime(collection.assetPath(
      fileLayerProviderCollection.items.head.id,
      fileLayerProviderCollection.items.head.assets.head.fileName))

    // Second call – must be a no-op
    StacTestGenerator.ensureGenerated(fileLayerProviderCollection, outputDir)
    val mtime2 = Files.getLastModifiedTime(collection.assetPath(
      fileLayerProviderCollection.items.head.id,
      fileLayerProviderCollection.items.head.assets.head.fileName))

    assertEquals(mtime, mtime2, "Files were regenerated unnecessarily")
  }

  @Test
  def toOpenSearchFeaturesReturnsOnePerItem(): Unit = {
    val features = collection.toOpenSearchFeatures
    assertEquals(fileLayerProviderCollection.items.size, features.size)
    features.foreach { f =>
      assertNotNull(f.bbox)
      assertNotNull(f.nominalDate)
      assertTrue(f.links.length == 3, s"Expected 3 links, got ${f.links.length}")
    }
  }

  @Test
  def collectionJsonContainsAllItems(): Unit = {
    val json = new String(Files.readAllBytes(collection.collectionJsonPath), "UTF-8")
    fileLayerProviderCollection.items.foreach { item =>
      assertTrue(json.contains(item.id), s"collection.json missing item ${item.id}")
    }
  }

  @Test
  def itemJsonContainsAbsolutePaths(): Unit = {
    val item = fileLayerProviderCollection.items.head
    val json = new String(
      Files.readAllBytes(collection.itemDir(item.id).resolve("item.json")), "UTF-8")
    item.assets.foreach { asset =>
      assertTrue(json.contains(asset.fileName), s"item.json missing ${asset.fileName}")
    }
  }

  @Test
  def itemJsonContainsProjExtensionFields(): Unit = {
    val item = fileLayerProviderCollection.items.head
    val json = new String(
      Files.readAllBytes(collection.itemDir(item.id).resolve("item.json")), "UTF-8")

    // Extension URI declared
    assertTrue(json.contains("projection/v1.1.0"), "Missing projection extension URI")

    // Each asset must carry proj:epsg, proj:shape, proj:bbox
    assertTrue(json.contains(""""proj:epsg":32631"""), "Missing proj:epsg for UTM asset")
    assertTrue(json.contains(""""proj:epsg":4326"""),  "Missing proj:epsg for LatLng NetCDF")
    assertTrue(json.contains("proj:shape"),            "Missing proj:shape")
    assertTrue(json.contains("proj:bbox"),             "Missing proj:bbox")

    // proj:shape for the 10m GeoTIFF: extent 2560×2560 m at 10m → 256×256 pixels → [256, 256]
    assertTrue(json.contains("[256,256]"), "Unexpected proj:shape for 10m GeoTIFF")
  }
}
