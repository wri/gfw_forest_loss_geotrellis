package org.globalforestwatch.layers

import cats.data.NonEmptyList

import java.io.FileNotFoundException
import cats.implicits._
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.AmazonS3Exception
import geotrellis.layer.{LayoutDefinition, LayoutTileSource, SpatialKey}
import geotrellis.raster.gdal.GDALRasterSource
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.util.Config
import org.globalforestwatch.util.Util.getAnyMapValue
import geotrellis.raster.{CellType, IntCells, NoDataHandling, Tile, isNoData}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{HeadObjectRequest, NoSuchKeyException, RequestPayer}
import org.globalforestwatch.grids.GridTile

import java.time.LocalDate

trait Layer {

  /**
    * Layers include information on how/ where to access specific raster data and how to interpret the pixel values
    */
  type A
  type B

  val s3Client: S3Client = geotrellis.store.s3.S3ClientProducer.get()

  val internalNoDataValue: A
  val externalNoDataValue: B
  val basePath: String = s"s3://gfw-data-lake"

  val kwargs: Map[String, Any]
  val datasetName: String

  val uri: String

  val pinnedVersions: Map[String, String] = {
    val pinnedVersionList: Option[NonEmptyList[Config]] =
      getAnyMapValue[Option[NonEmptyList[Config]]](kwargs, "pinnedVersions")

    pinnedVersionList match {
      case Some(versions) =>
        versions.foldLeft(Map(): Map[String, String])(
          (acc, pinnedVersion) =>
            acc + (pinnedVersion.key -> pinnedVersion.value)
        )
      case _ => Map()
    }
  }

  protected def uriForGrid(grid: GridTile, kwargs: Map[String, Any]): String = {
    val config: GfwConfig = getAnyMapValue[GfwConfig](kwargs, "config")
    val baseUri = config.rasterCatalog.getSourceUri(datasetName, grid)
    baseUri.replace("{grid_size}", grid.gridSize.toString)
      .replace("{row_count}", grid.rowCount.toString)
      .replace("{tile_id}", grid.tileId)
  }

  def lookup(a: A): B
}

trait ILayer extends Layer {

  /**
    * Wrapper for interger rasters to assure that data according to layer specifications are returned
    */
  type A = Int

  implicit class ITile(val t: Tile) {
    def getData(col: Int, row: Int): B = {
      val value: Int = t.get(col, row)
      if (isNoData(value)) externalNoDataValue else lookup(value)
    }
    def cellType: CellType = t.cellType
    def cols: Int = t.cols
    def rows: Int = t.rows

    val noDataValue: B = externalNoDataValue
  }

  implicit class OptionalITile(val t: Option[Tile]) {
    def getData(col: Int, row: Int): B = {
      val value: Int = t.map(_.get(col, row)).getOrElse(internalNoDataValue)
      if (isNoData(value) || value == internalNoDataValue) externalNoDataValue
      else lookup(value)
    }

    def getCoverage(col: Int, row: Int): Boolean = {
      t.map(_.get(col, row)).isDefined
    }

    type I = IntCells with NoDataHandling

    def cellType: Option[CellType] = t match {
      case Some(tile) => Some(tile.cellType)
      case _ => None
    }

    def cols: Option[Int] = t match {
      case Some(tile) => Some(tile.cols)
      case _ => None
    }

    def rows: Option[Int] = t match {
      case Some(tile) => Some(tile.rows)
      case _ => None
    }

    val noDataValue: B = externalNoDataValue
  }

}

trait DLayer extends Layer {

  /**
    * Wrapper for double rasters to assure that data according to layer specifications are returned
    */
  type A = Double

  implicit class DTile(val t: Tile) {
    def getData(col: Int, row: Int): B = {
      val value: Double = t.getDouble(col, row)
      if (isNoData(value)) externalNoDataValue else lookup(value)
    }
    def cellType: CellType = t.cellType
    def cols: Int = t.cols
    def rows: Int = t.rows

    val noDataValue: B = externalNoDataValue
  }

  implicit class OptionalDTile(val t: Option[Tile]) {
    def getData(col: Int, row: Int): B = {
      val value: Double =
        t.map(_.getDouble(col, row)).getOrElse(internalNoDataValue)
      if (isNoData(value) || value == internalNoDataValue) externalNoDataValue
      else lookup(value)
    }
    def cellType: CellType = t.cellType
    def cols: Int = t.cols
    def rows: Int = t.rows

    val noDataValue: B = externalNoDataValue
  }

}

trait FLayer extends Layer {

  /**
    * Wrapper for double rasters to assure that data according to layer specifications are returned
    */
  type A = Float

  implicit class FTile(val t: Tile) {
    def getData(col: Int, row: Int): B = {
      val value: Float = t.getDouble(col, row).toFloat
      if (isNoData(value)) externalNoDataValue else lookup(value)
    }

    def cellType: CellType = t.cellType

    def cols: Int = t.cols

    def rows: Int = t.rows

    val noDataValue: B = externalNoDataValue
  }

  implicit class OptionalFTile(val t: Option[Tile]) {
    def getData(col: Int, row: Int): B = {
      val value: Float =
        t.map(_.getDouble(col, row).toFloat).getOrElse(internalNoDataValue)
      if (isNoData(value) || value == internalNoDataValue) externalNoDataValue
      else lookup(value)
    }

    def cellType: CellType = t.cellType

    def cols: Int = t.cols

    def rows: Int = t.rows

    val noDataValue: B = externalNoDataValue
  }

}

trait RequiredLayer extends Layer {

  /**
    * Define how to read sources for required layers
    */
  lazy val source: GDALRasterSource = {
    // Removes the expected 404 errors from console log
    val s3uri = new AmazonS3URI(uri)
    try {
      val keyExists =
        try {
          val headRequest = HeadObjectRequest
            .builder()
            .bucket(s3uri.getBucket)
            .key(s3uri.getKey)
            .requestPayer(RequestPayer.REQUESTER)
            .build()
          s3Client.headObject(headRequest)
          true
        } catch {
          case noSuchKeyE: NoSuchKeyException => false
          case e: Throwable => throw e
        }

      if (!keyExists) {
        println(s"Cannot open: $uri")
        throw new FileNotFoundException(uri)
      }

      println(s"Opening: $uri")
    } catch {
      case e: AmazonS3Exception => throw new FileNotFoundException(uri)
    }
    GDALRasterSource(uri)
  }
}

trait RequiredILayer extends RequiredLayer with ILayer {

  /**
    * Define how to fetch data for required Integer rasters
    */
  def fetchWindow(windowKey: SpatialKey,
                  windowLayout: LayoutDefinition): ITile = {
    val layoutSource = LayoutTileSource.spatial(source, windowLayout)
//    println(s"Fetching required int tile ${source.dataPath.value}, key ${windowKey}")
    val tile = source.synchronized {
      layoutSource.read(windowKey).get.band(0)
    }
    new ITile(tile)
  }

}

trait RequiredDLayer extends RequiredLayer with DLayer {

  /**
    * Define how to fetch data for required Double rasters
    */
  def fetchWindow(windowKey: SpatialKey,
                  windowLayout: LayoutDefinition): DTile = {
    val layoutSource = LayoutTileSource.spatial(source, windowLayout)
//    println(s"Fetching required int tile ${source.dataPath.value}, key ${windowKey}")
    val tile = source.synchronized {
      layoutSource.read(windowKey).get.band(0)
    }
    new DTile(tile)
  }

}

trait RequiredFLayer extends RequiredLayer with FLayer {

  /**
    * Define how to fetch data for required Double rasters
    */
  def fetchWindow(windowKey: SpatialKey,
                  windowLayout: LayoutDefinition): FTile = {
    val layoutSource = LayoutTileSource.spatial(source, windowLayout)
//    println(s"Fetching required float tile ${source.dataPath.value}, key ${windowKey}")
    val tile = source.synchronized {
      layoutSource.read(windowKey).get.band(0)
    }
    new FTile(tile)
  }

}

trait OptionalLayer extends Layer {

  /**
    * Define how to read sources for optional Layers
    */
  /** Check if URI exists before trying to open it, return None if no file found */
  lazy val source: Option[GDALRasterSource] = {
    // Removes the expected 404 errors from console log

    val s3uri = new AmazonS3URI(uri)
    try {
      val keyExists =
        try {
          val headRequest = HeadObjectRequest
            .builder()
            .bucket(s3uri.getBucket)
            .key(s3uri.getKey)
            .requestPayer(RequestPayer.REQUESTER)
            .build()
          s3Client.headObject(headRequest)
          true
        } catch {
          case e: NoSuchKeyException => false
          case any: Throwable => throw any
        }

      if (keyExists) {
        println(s"Opening: $uri")
        Some(GDALRasterSource(uri))
      } else {
        println(s"Cannot open: $uri")
        None
      }
    } catch {
      case e: AmazonS3Exception =>
        println(s"Cannot open: $uri")
        None
    }
  }
}

trait OptionalILayer extends OptionalLayer with ILayer {

  /**
    * Define how to fetch data for optional Integer rasters
    */
  def fetchWindow(windowKey: SpatialKey,
                  windowLayout: LayoutDefinition): OptionalITile = {
//    source.foreach(s => println(s"Fetching optional int tile ${s.dataPath.value}, key ${windowKey}"))
    new OptionalITile(for {
      source <- source
      raster <- Either
        .catchNonFatal(source.synchronized {
          LayoutTileSource
            .spatial(source, windowLayout)
            .read(windowKey)
            .get
            .band(0)
        })
        .toOption
    } yield raster)
  }
}

trait OptionalDLayer extends OptionalLayer with DLayer {

  /**
    * Define how to fetch data for optional double rasters
    */
  def fetchWindow(windowKey: SpatialKey,
                  windowLayout: LayoutDefinition): OptionalDTile = {
//    source.foreach(s => println(s"Fetching optional double tile ${s.dataPath.value}, key ${windowKey}"))
    new OptionalDTile(for {
      source <- source
      raster <- Either
        .catchNonFatal(source.synchronized {
          LayoutTileSource
            .spatial(source, windowLayout)
            .read(windowKey)
            .get
            .band(0)
        })
        .toOption
    } yield raster)
  }
}

trait OptionalFLayer extends OptionalLayer with FLayer {

  /**
    * Define how to fetch data for optional double rasters
    */
  def fetchWindow(windowKey: SpatialKey,
                  windowLayout: LayoutDefinition): OptionalFTile = {
//    source.foreach(s => println(s"Fetching optional float tile ${s.dataPath.value}, key ${windowKey}"))
    new OptionalFTile(for {
      source <- source
      raster <- Either
        .catchNonFatal(source.synchronized {
          LayoutTileSource
            .spatial(source, windowLayout)
            .read(windowKey)
            .get
            .band(0)
        })
        .toOption
    } yield raster)
  }
}

trait BooleanLayer extends ILayer {

  /**
    * Layers which return a Boolean type
    */
  type B = Boolean

  val internalNoDataValue: Int = 0
  val externalNoDataValue: Boolean = false

  def lookup(value: Int): Boolean = if (value == 0) false else true
}

trait IntegerLayer extends ILayer {

  /**
    * Layers which return an Integer type
    * (use java.lang.Integer to be able to use null)
    */
  type B = Integer

  val internalNoDataValue: Int = 0
  val externalNoDataValue: Integer = null

  def lookup(value: Int): Integer = value
}

// Encoding for RaddAlerts, GladAlerts, and GladAlertsS2 layers. Confidence boolean
// value of true means "high" confidence, else confidence is "nominal"/"low".
trait DateConfLayer extends ILayer {
  type B = Option[(LocalDate, Boolean)]

  val internalNoDataValue: Int = 0
  val externalNoDataValue: B = None

  val baseDate = LocalDate.of(2014,12,31)

  override def lookup(value: Int): Option[(LocalDate, Boolean)] = {
    val confidence = value >= 30000
    val days: Int = if (confidence) value - 30000 else value - 20000
    if (days < 0) {
      None
    } else {
      val date = baseDate.plusDays(days)
      Some((date, confidence))
    }
  }
}

// Encoding for IntegratedAlerts. Confidence value of 2 means "highest", 1 means
// "high", and 0 means "nominal".
trait DateConfLevelsLayer extends ILayer {
  type B = Option[(LocalDate, Int)]

  val internalNoDataValue: Int = 0
  val externalNoDataValue: B = None

  val baseDate = LocalDate.of(2014,12,31)

  override def lookup(value: Int): Option[(LocalDate, Int)] = {
    val confidence = if (value >= 40000) 2 else if (value >= 30000) 1 else 0
    val days: Int = if (value >= 40000) value - 40000
      else if (value >= 30000) value - 30000
      else value - 20000
    if (days < 0) {
      None
    } else {
      val date = baseDate.plusDays(days)
      Some((date, confidence))
    }
  }
}

trait DIntegerLayer extends DLayer {

  /**
    * Layers which return an Integer type
    * (use java.lang.Integer to be able to use null)
    */
  type B = Integer

  val internalNoDataValue: Double = 0
  val externalNoDataValue: Integer = null

}

trait FIntegerLayer extends FLayer {

  /**
    * Layers which return an Integer type
    * (use java.lang.Integer to be able to use null)
    */
  type B = Integer

  val internalNoDataValue: Float = 0
  val externalNoDataValue: Integer = null

}

trait DBooleanLayer extends DLayer {

  /**
    * Layers which return an Integer type
    * (use java.lang.Integer to be able to use null)
    */
  type B = Boolean

  val internalNoDataValue: Double = 0
  val externalNoDataValue: Boolean = false

}

trait FBooleanLayer extends FLayer {

  /**
    * Layers which return an Integer type
    * (use java.lang.Integer to be able to use null)
    */
  type B = Boolean

  val internalNoDataValue: Float = 0
  val externalNoDataValue: Boolean = false

}

trait DoubleLayer extends DLayer {

  /**
    * Layers which return a Double type
    */
  type B = Double

  val internalNoDataValue: Double = 0
  val externalNoDataValue: Double = 0

  def lookup(value: Double): Double = value
}

trait FloatLayer extends FLayer {

  /**
    * Layers which return a Double type
    */
  type B = Float

  val internalNoDataValue: Float = 0
  val externalNoDataValue: Float = 0

  def lookup(value: Float): Float = value
}

trait StringLayer extends ILayer {

  /**
    * Layers which return a String type
    */
  type B = String

  val internalNoDataValue: Int = 0
  val externalNoDataValue: String = ""

}

trait MapILayer extends ILayer {

  /**
    * Layers which return a String type
    */
  type B = Map[String, Boolean]

  val internalNoDataValue: Int = 0
  val externalNoDataValue: Map[String, Boolean] = Map()

}

trait MapFLayer extends FLayer {

  /**
    * Layers which return a String type
    */
  type B = Map[String, Boolean]

  val internalNoDataValue: Float = 0
  val externalNoDataValue: Map[String, Boolean] = Map()

}

// Type representing a forest loss year which could be approximate. If approx is
// value, the tree loss occurred exactly in the specified year. If approx is false,
// then the tree loss occurred in a multi-year range ending in the specified year.
//
// We define compare() so ApproxYear can be a key in an OrderedMap.
case class ApproxYear(year: Int, approx: Boolean) extends Ordered[ApproxYear] {

  def compare(that: ApproxYear): Int = Ordering.Tuple2[Int, Boolean].compare((this.year, this.approx), (that.year, that.approx))
}

trait ApproxYearLayer extends ILayer {
  type B = ApproxYear

  val internalNoDataValue: Int = 0
  val externalNoDataValue: ApproxYear = ApproxYear(0, false)
}
