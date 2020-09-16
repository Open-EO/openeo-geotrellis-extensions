package org.openeo.geotrellisaccumulo.zcurve

import geotrellis.layer.{KeyBounds, SpaceTimeKey}
import geotrellis.store.index.KeyIndex
import org.locationtech.sfcurve.zorder.{Z3, ZRange}

object SfCurveZSpaceTimeKeyIndex {
  def byMilliseconds(keyBounds: KeyBounds[SpaceTimeKey], millis: Long): SfCurveZSpaceTimeKeyIndex =
    new SfCurveZSpaceTimeKeyIndex(keyBounds, millis)

  def bySecond(keyBounds: KeyBounds[SpaceTimeKey]): SfCurveZSpaceTimeKeyIndex =
    byMilliseconds(keyBounds, 1000L)

  def bySeconds(keyBounds: KeyBounds[SpaceTimeKey], seconds: Int): SfCurveZSpaceTimeKeyIndex =
    byMilliseconds(keyBounds, 1000L * seconds)

  def byMinute(keyBounds: KeyBounds[SpaceTimeKey]): SfCurveZSpaceTimeKeyIndex =
    byMilliseconds(keyBounds, 1000L * 60)

  def byMinutes(keyBounds: KeyBounds[SpaceTimeKey], minutes: Int): SfCurveZSpaceTimeKeyIndex =
    byMilliseconds(keyBounds, 1000L * 60 * minutes)

  def byHour(keyBounds: KeyBounds[SpaceTimeKey]): SfCurveZSpaceTimeKeyIndex =
    byMilliseconds(keyBounds, 1000L * 60 * 60)

  def byHours(keyBounds: KeyBounds[SpaceTimeKey], hours: Int): SfCurveZSpaceTimeKeyIndex =
    byMilliseconds(keyBounds, 1000L * 60 * 60 * hours)

  def byDay(keyBounds: KeyBounds[SpaceTimeKey]): SfCurveZSpaceTimeKeyIndex =
    byMilliseconds(keyBounds, 1000L * 60 * 60 * 24)

  def byDays(keyBounds: KeyBounds[SpaceTimeKey], days: Int): SfCurveZSpaceTimeKeyIndex =
    byMilliseconds(keyBounds, 1000L * 60 * 60 * 24 * days)

  def byMonth(keyBounds: KeyBounds[SpaceTimeKey]): SfCurveZSpaceTimeKeyIndex =
    byMilliseconds(keyBounds, 1000L * 60 * 60 * 24 * 30)

  def byMonths(keyBounds: KeyBounds[SpaceTimeKey], months: Int): SfCurveZSpaceTimeKeyIndex =
    byMilliseconds(keyBounds, 1000L * 60 * 60 * 24 * 30 * months)

  def byYear(keyBounds: KeyBounds[SpaceTimeKey]): SfCurveZSpaceTimeKeyIndex =
    byMilliseconds(keyBounds, 1000L * 60 * 60 * 24 * 365)

  def byYears(keyBounds: KeyBounds[SpaceTimeKey], years: Int): SfCurveZSpaceTimeKeyIndex =
    byMilliseconds(keyBounds, 1000L * 60 * 60 * 24 * 365 * years)
}

class SfCurveZSpaceTimeKeyIndex(val keyBounds: KeyBounds[SpaceTimeKey], val temporalResolution: Long) extends KeyIndex[SpaceTimeKey] {
  private def toZ(key: SpaceTimeKey): Z3 = Z3(key.col, key.row, (key.instant / temporalResolution).toInt)

  def toIndex(key: SpaceTimeKey): BigInt = toZ(key).z

  def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] =
    indexRanges(keyRange, 11)

  def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey), maxRecurse: Int): Seq[(BigInt, BigInt)] =
    Z3.zranges(Array(ZRange(toZ(keyRange._1), toZ(keyRange._2))), maxRecurse = Some(maxRecurse)).map(r => (BigInt(r.lower), BigInt(r.upper)))
}
