package org.openeo.geotrellis

import geotrellis.vector.Extent

/**
 * Options shared by the output formats (GTiff, NetCDF, PNG, Zarr).
 *
 * Not every format uses every option yet; unused options are ignored by the respective writer.
 */
trait FormatOptions extends Serializable {

  /**
   * Temporal extent requested for the result (e.g. from `load_collection` or `filter_temporal`).
   * Used for the item's dates when the result itself carries no date (e.g. spatial cubes).
   */
  var temporalExtent: Option[(String, String)] = Option.empty

  // TODO: the GTiff and PNG writers take the crop bounds as a separate `cropBounds` parameter, duplicating this
  //  option. Switching them to `options.cropBounds` changes method signatures called by openeo-geopyspark-driver,
  //  so coordinate with the driver.
  var cropBounds: Option[Extent] = Option.empty[Extent]

  var addBandStatistics: Boolean = true

  var retainNoDataTiles: Boolean = false

  /** Either end can be null (open ended). */
  def setTemporalExtent(start: String, end: String): Unit =
    temporalExtent = if (start == null && end == null) Option.empty else Some((start, end))

  def startDatetime: String = temporalExtent.map(_._1).orNull

  def endDatetime: String = temporalExtent.map(_._2).orNull

  def setCropBounds(extent: Extent): Unit = cropBounds = Option(extent)

  def setAddBandStatistics(enabled: Boolean): Unit = addBandStatistics = enabled

  def setRetainNoDataTiles(enabled: Boolean): Unit = retainNoDataTiles = enabled
}

