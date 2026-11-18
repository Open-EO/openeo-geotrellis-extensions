package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.raster.CellSize
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.{Feature, FeatureCollection, Link}
import org.openeo.opensearch.OpenSearchClient

import java.net.URI
import java.time.ZonedDateTime

class BandAssetLinkResolverTest {

  private val bbox = Extent(0.0, 0.0, 1.0, 1.0)
  private val nominalDate = ZonedDateTime.parse("2024-01-01T00:00:00Z")

  private def link(href: String, title: String, bandNames: Seq[String]): Link =
    Link(href = URI.create(href), title = Some(title), bandNames = Some(bandNames))

  private def feature(id: String, links: Array[Link]): Feature =
    Feature(id = id, bbox = bbox, nominalDate = nominalDate, links = links, resolution = Some(10.0), collectionId = "S2")

  @Test
  def resolvesBandsFromBandInfoInSingleStacAsset(): Unit = {
    val stacFeature = feature(
      id = "scene-1",
      links = Array(link("file:///tmp/multi.tif", "BANDS", Seq("B02", "B03")))
    )

    val client = new FixedFeaturesOpenSearchClient()
    client.addFeature(stacFeature)

    val resolver = BandAssetLinkResolver(
      openSearch = client,
      openSearchLinkTitles = NonEmptyList.of("B03", "B02"),
      rootPath = "/tmp",
      maxSpatialResolution = CellSize(10, 10),
      bandIndices = Seq.empty,
      experimental = false,
      maxSoftErrorsRatio = 0.0
    )

    val assets = resolver.getBandAssets(stacFeature)

    assertEquals(2, assets.size)
    assertTrue(assets.forall(_.isDefined))

    val Some((firstLink, firstBandIndex, firstBandName)) = assets.head
    assertEquals("file:///tmp/multi.tif", firstLink.href.toString)
    assertEquals("B03", firstBandName)
    assertEquals(1, firstBandIndex)

    val Some((secondLink, secondBandIndex, secondBandName)) = assets(1)
    assertEquals("file:///tmp/multi.tif", secondLink.href.toString)
    assertEquals("B02", secondBandName)
    assertEquals(0, secondBandIndex)
  }

  @Test
  def returnsNoneWhenARequestedBandIsMissingInStacAssets(): Unit = {
    val stacFeature = feature(
      id = "scene-missing",
      links = Array(link("file:///tmp/multi.tif", "BANDS", Seq("B02", "B03")))
    )

    val client = new FixedFeaturesOpenSearchClient()
    client.addFeature(stacFeature)

    val resolver = BandAssetLinkResolver(
      openSearch = client,
      openSearchLinkTitles = NonEmptyList.of("B02", "B08"),
      rootPath = "/tmp",
      maxSpatialResolution = CellSize(10, 10),
      bandIndices = Seq.empty,
      experimental = false,
      maxSoftErrorsRatio = 0.0
    )

    val assets = resolver.getBandAssets(stacFeature)

    assertEquals(2, assets.size)
    assertTrue(assets.head.isDefined)
    assertFalse(assets(1).isDefined)
  }

  @Test
  def mergesIncompleteStacFeaturesForTheSameScene(): Unit = {
    val featureWithB02 = feature(
      id = "scene-1",
      links = Array(link("file:///tmp/B02.tif", "B02", Seq("B02")))
    )
    val featureWithB03 = feature(
      id = "scene-2",
      links = Array(link("file:///tmp/B03.tif", "B03", Seq("B03")))
    )

    val client = new FixedFeaturesOpenSearchClient()
    client.addFeature(featureWithB02)
    client.addFeature(featureWithB03)

    val resolver = BandAssetLinkResolver(
      openSearch = client,
      openSearchLinkTitles = NonEmptyList.of("B02", "B03"),
      rootPath = "/tmp",
      maxSpatialResolution = CellSize(10, 10),
      bandIndices = Seq.empty,
      experimental = false,
      maxSoftErrorsRatio = 0.0
    )

    val assets = resolver.getBandAssets(featureWithB02)
    val resolvedHrefs = assets.flatten.map(_._1.href.toString).toSet

    assertEquals(2, assets.count(_.isDefined))
    assertTrue(resolvedHrefs.contains("file:///tmp/B02.tif"))
    assertTrue(resolvedHrefs.contains("file:///tmp/B03.tif"))
  }

  @Test
  def resolvesByLinkTitleWithConfiguredBandIndicesForNonStacClients(): Unit = {
    val resolver = BandAssetLinkResolver(
      openSearch = DummyOpenSearchClient,
      openSearchLinkTitles = NonEmptyList.of("ASSET_RED", "ASSET_NIR"),
      rootPath = "/tmp",
      maxSpatialResolution = CellSize(10, 10),
      bandIndices = Seq(4, 7),
      experimental = false,
      maxSoftErrorsRatio = 0.0
    )

    val item = feature(
      id = "scene-3",
      links = Array(
        link("file:///tmp/red.tif", "asset_red", Seq("red")),
        link("file:///tmp/nir.tif", "asset_nir", Seq("nir"))
      )
    )

    val assets = resolver.getBandAssets(item)

    assertEquals(2, assets.size)
    assertTrue(assets.forall(_.isDefined))
    assertEquals(4, assets.head.get._2)
    assertEquals("ASSET_RED", assets.head.get._3)
    assertEquals(7, assets(1).get._2)
    assertEquals("ASSET_NIR", assets(1).get._3)
  }
}

object DummyOpenSearchClient extends OpenSearchClient {
  override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[Feature] = Seq.empty

  override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, page: Int): FeatureCollection = ???

  override def getCollections(correlationId: String): Seq[Feature] = ???

  override def equals(obj: Any): Boolean = obj match {
    case that: DummyOpenSearchClient.type => this eq that
    case _ => false
  }

  override def hashCode(): Int = System.identityHashCode(this)
}

