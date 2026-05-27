package org.openeo.geotrellishealpix

import healpix.{HealpixBase, Pointing, Scheme}
import org.apache.spark.sql.types.{DataType, DoubleType}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.Timestamp
import java.time.ZonedDateTime
import scala.jdk.CollectionConverters._
import scala.util.Random

/** Helpers to populate a HealpixDatacube with synthetic data. */
object HealpixDataGenerator {

  private def depthOf(nside: Int): Int = {
    val d = (math.log(nside.toDouble) / math.log(2.0)).round.toInt
    require(1 << d == nside, s"NSIDE must be a power of two, got $nside")
    d
  }

  private def newBase(nside: Int): HealpixBase = new HealpixBase(nside, Scheme.NESTED)

  /** Random uniform [0,1) values. */
  def randomScalar(spark: SparkSession,
                   nside: Int,
                   timestamps: Seq[Timestamp],
                   bands: Seq[String],
                   seed: Long = 42L): ScalarHealpixDatacube = {
    val bandDefs = bands.map(_ -> (DoubleType: DataType))
    val schema   = HealpixSchema.scalarSchema(bandDefs)
    val npix     = 12L * nside.toLong * nside.toLong
    val rng      = new Random(seed)

    val rows = for {
      ts  <- timestamps
      cid <- 0L until npix
    } yield Row.fromSeq(Seq[Any](cid, ts) ++ bands.map(_ => rng.nextDouble()))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    ScalarHealpixDatacube(nside, bandDefs, df)
  }

  /**
   * Deterministic "fractal" generator: a midpoint-displacement style 1D walk
   * along the HEALPix nested cell index, giving spatially coherent values.
   *
   * @param childrenPerParent must be a power of 4; each packed row represents one parent cell.
   */
  def fractalPacked(spark: SparkSession,
                    nside: Int,
                    chunkSize: Int,
                    timestamps: Seq[Timestamp],
                    bands: Seq[String],
                    seed: Long = 1234L): PackedHealpixDatacube = {
    val childrenPerParent = chunkSize
    require(PackedHealpixDatacube.isPowerOf4(childrenPerParent),
      s"chunkSize (childrenPerParent) must be a power of 4, got $childrenPerParent")

    val bandDefs = bands.map(_ -> (DoubleType: DataType))
    val schema   = HealpixSchema.packedSchema(bandDefs)
    val npix     = 12L * nside.toLong * nside.toLong

    def walk(start: Long, len: Int, salt: Int): Array[java.lang.Double] = {
      val r = new Random(seed + salt + start)
      val arr = new Array[java.lang.Double](len)
      var v = r.nextDouble()
      var i = 0
      while (i < len) {
        v = math.min(1.0, math.max(0.0, v + (r.nextDouble() - 0.5) * 0.1))
        arr(i) = java.lang.Double.valueOf(v)
        i += 1
      }
      arr
    }

    val rows = for {
      ts    <- timestamps
      start <- 0L until npix by childrenPerParent.toLong
    } yield {
      val len = math.min(childrenPerParent.toLong, npix - start).toInt
      val bandData: Seq[Any] = bands.zipWithIndex.map { case (_, bi) =>
        walk(start, len, bi).toSeq
      }
      Row.fromSeq(Seq[Any](start, len, ts) ++ bandData)
    }

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    PackedHealpixDatacube(nside, bandDefs, childrenPerParent, df)
  }

  /**
   * Deterministic "stripes" generator (scalar layout): value = sin(lat) for the
   * cell center. Useful for visually verifying reprojection / rendering.
   */
  def latitudeStripesScalar(spark: SparkSession,
                            nside: Int,
                            timestamps: Seq[Timestamp],
                            band: String = "lat"): ScalarHealpixDatacube = {
    val bandDefs = Seq(band -> (DoubleType: DataType))
    val schema   = HealpixSchema.scalarSchema(bandDefs)
    val npix     = 12L * nside.toLong * nside.toLong
    val base     = newBase(nside)

    val rows = for {
      ts  <- timestamps
      cid <- 0L until npix
    } yield {
      val p: Pointing = base.pix2ang(cid)
      // Pointing.theta is colatitude in radians: latitude = pi/2 - theta
      val latRad = math.Pi / 2.0 - p.theta
      Row.fromSeq(Seq[Any](cid, ts, math.sin(latRad)))
    }
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    ScalarHealpixDatacube(nside, bandDefs, df)
  }

  // ---- Java-friendly overloads (using java.util.List and ISO 8601 strings) ----

  /**   * Java-friendly version of randomScalar using java.util.List and ISO 8601 timestamp strings.   *   * @param spark active SparkSession   * @param nside HEALPix NSIDE (power of two)   * @param timestamps list of ISO 8601 timestamp strings (e.g., "2024-05-27T10:30:00Z")   * @param bands list of band/variable names   * @param seed random seed (default: 42)   * @return ScalarHealpixDatacube with random values   */
  def randomScalar(spark: SparkSession,
                   nside: Int,
                   timestamps: java.util.List[String],
                   bands: java.util.List[String],
                   seed: Long): ScalarHealpixDatacube = {
    val scalaTimestamps = timestamps.asScala.toSeq.map(parseTimestamp)
    val scalaBands = bands.asScala.toSeq
    randomScalar(spark, nside, scalaTimestamps, scalaBands, seed)
  }

  /**   * Java-friendly version of randomScalar with default seed.   */
  def randomScalar(spark: SparkSession,
                   nside: Int,
                   timestamps: java.util.List[String],
                   bands: java.util.List[String]): ScalarHealpixDatacube = {
    randomScalar(spark, nside, timestamps, bands, 42L)
  }

  /**   * Java-friendly version of fractalPacked using java.util.List and ISO 8601 strings.   *   * @param spark active SparkSession   * @param nside HEALPix NSIDE (power of two)   * @param chunkSize children per parent (must be power of 4)   * @param timestamps list of ISO 8601 timestamp strings   * @param bands list of band/variable names   * @param seed random seed (default: 1234)   * @return PackedHealpixDatacube   */
  def fractalPacked(spark: SparkSession,
                    nside: Int,
                    chunkSize: Int,
                    timestamps: java.util.List[String],
                    bands: java.util.List[String],
                    seed: Long): PackedHealpixDatacube = {
    val scalaTimestamps = timestamps.asScala.toSeq.map(parseTimestamp)
    val scalaBands = bands.asScala.toSeq
    fractalPacked(spark, nside, chunkSize, scalaTimestamps, scalaBands, seed)
  }

  /**   * Java-friendly version of fractalPacked with default seed.   */
  def fractalPacked(spark: SparkSession,
                    nside: Int,
                    chunkSize: Int,
                    timestamps: java.util.List[String],
                    bands: java.util.List[String]): PackedHealpixDatacube = {
    fractalPacked(spark, nside, chunkSize, timestamps, bands, 1234L)
  }

  /**   * Java-friendly version of latitudeStripesScalar using java.util.List and ISO 8601 strings.   *   * @param spark active SparkSession   * @param nside HEALPix NSIDE (power of two)   * @param timestamps list of ISO 8601 timestamp strings   * @param band band/variable name (default: "lat")   * @return ScalarHealpixDatacube with latitude-based values   */
  def latitudeStripesScalar(spark: SparkSession,
                            nside: Int,
                            timestamps: java.util.List[String],
                            band: String): ScalarHealpixDatacube = {
    val scalaTimestamps = timestamps.asScala.toSeq.map(parseTimestamp)
    latitudeStripesScalar(spark, nside, scalaTimestamps, band)
  }

  /**   * Java-friendly version of latitudeStripesScalar with default band name.   */
  def latitudeStripesScalar(spark: SparkSession,
                            nside: Int,
                            timestamps: java.util.List[String]): ScalarHealpixDatacube = {
    latitudeStripesScalar(spark, nside, timestamps, "lat")
  }

  // ---- Helper: parse ISO 8601 timestamp string ----

  /**   * Parse an ISO 8601 timestamp string to java.sql.Timestamp.   * Examples: "2024-05-27T10:30:00Z", "2024-05-27T10:30:00+00:00"   */
  private def parseTimestamp(iso8601: String): Timestamp = {
    try {
      val zdt = ZonedDateTime.parse(iso8601)
      Timestamp.from(zdt.toInstant)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          s"Invalid ISO 8601 timestamp: '$iso8601' (e.g., '2024-05-27T10:30:00Z')",
          e
        )
    }
  }
}
