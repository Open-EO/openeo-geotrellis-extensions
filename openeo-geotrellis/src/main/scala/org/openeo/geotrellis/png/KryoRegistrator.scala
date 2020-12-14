package org.openeo.geotrellis.png

import ar.com.hjg.pngj.{ImageInfo, ImageLineInt}
import com.esotericsoftware.kryo.Kryo
import geotrellis.raster.RasterRegion.GridBoundsRasterRegion

class KryoRegistrator extends org.apache.spark.serializer.KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[ImageInfo])
    kryo.register(classOf[ImageLineInt])
    kryo.register(classOf[GridBoundsRasterRegion])
  }
}
