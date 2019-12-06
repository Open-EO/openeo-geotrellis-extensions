package be.vito.eodata.geopysparkextensions

import java.net.URI

import geotrellis.spark.store.accumulo._
import geotrellis.spark.store.{FilteringLayerReader, LayerReaderProvider, LayerWriter, LayerWriterProvider}
import geotrellis.store.accumulo.conf.AccumuloConfig
import geotrellis.store.accumulo.{AccumuloAttributeStore, AccumuloCollectionLayerReader, AccumuloValueReader}
import geotrellis.store.{AttributeStore, AttributeStoreProvider, CollectionLayerReader, CollectionLayerReaderProvider, LayerId, ValueReader, ValueReaderProvider}
import geotrellis.util.UriUtils
import org.apache.spark.SparkContext

/**
  * Provides [[geotrellis.store.accumulo.AccumuloAttributeStore]] instance for URI with `accumulo+kerberos` scheme.
  *  ex: `accumulo://[user[:password]@]zookeeper/instance-name[?attributes=table1[&layers=table2]]`
  *
  * Attributes table name is optional, not provided default value will be used.
  * Layers table name is required to instantiate a [[geotrellis.spark.store.LayerWriter]]
  */
class KerberizedAccumuloLayerProvider extends AttributeStoreProvider
  with LayerReaderProvider with LayerWriterProvider with ValueReaderProvider with CollectionLayerReaderProvider {

  println(s"!!! instantiated a ${getClass.getName}")

  def canProcess(uri: URI): Boolean = uri.getScheme match {
    case str: String => if (str.toLowerCase == "accumulo+kerberos") true else false
    case null => false
  }

  def attributeStore(uri: URI): AttributeStore = {
    val instance = KerberizedAccumuloInstance(uri)
    val params = UriUtils.getParams(uri)
    val attributeTable = params.getOrElse("attributes", AccumuloConfig.catalog)
    AccumuloAttributeStore(instance, attributeTable)
  }

  def layerReader(uri: URI, store: AttributeStore, sc: SparkContext): FilteringLayerReader[LayerId] = {
    val instance = KerberizedAccumuloInstance(uri)

    new AccumuloLayerReader(store)(sc, instance)
  }

  def layerWriter(uri: URI, store: AttributeStore): LayerWriter[LayerId] = {
    val instance = KerberizedAccumuloInstance(uri)
    val params = UriUtils.getParams(uri)
    val table = params.getOrElse("layers",
      throw new IllegalArgumentException("Missing required URI parameter: layers"))

    AccumuloLayerWriter(instance, store, table)
  }

  def valueReader(uri: URI, store: AttributeStore): ValueReader[LayerId] = {
    val instance = KerberizedAccumuloInstance(uri)
    new AccumuloValueReader(instance, store)
  }

  def collectionLayerReader(uri: URI, store: AttributeStore): CollectionLayerReader[LayerId] = {
    val instance = KerberizedAccumuloInstance(uri)
    new AccumuloCollectionLayerReader(store)(instance)
  }

}