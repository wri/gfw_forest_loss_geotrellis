package org.globalforestwatch.layers

import cats.data.NonEmptyList

import java.io.FileNotFoundException
import cats.implicits._
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.AmazonS3Exception
import geotrellis.layer.{LayoutDefinition, LayoutTileSource, SpatialKey}
import geotrellis.raster.gdal.GDALRasterSource
import geotrellis.raster.{CellType, IntCells, NoDataHandling, Tile, isNoData}
import org.globalforestwatch.config.DataEnvironment
import org.globalforestwatch.util.Config
import org.globalforestwatch.util.Util.{getAnyMapValue, jsonStrToMap}
import software.amazon.awssdk.services.s3.S3Client
import scalaj.http._

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

  lazy val dataEnvironment: DataEnvironment = {
    val dataEnvironmentConfig: String =
      getAnyMapValue[String](kwargs, "dataEnvironment")

    DataEnvironment.getDataEnvironment(dataEnvironmentConfig)
  }

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

  lazy val version: String = {
    pinnedVersions.getOrElse(datasetName, None) match {
      case value: String => value
      case _ =>
        val response: HttpResponse[String] = Http(
          s"https://data-api.globalforestwatch.org/dataset/${datasetName}/latest"
        ).option(HttpOptions
          .followRedirects(true)).option(HttpOptions.connTimeout(10000)).option(HttpOptions.readTimeout(50000)).asString
        if (response.code != 200)
          throw new IllegalArgumentException(
            s"Dataset ${datasetName} has no latest version or does not exit. Data API responsen code: ${response.code}"
          )

        val json: Map[String, Any] = jsonStrToMap(response.body)

        val data = json.get("data").asInstanceOf[Option[Map[String, Any]]]
        data match {
          case Some(map) =>
            val version = map.get("version").asInstanceOf[Option[String]]
            version match {
              case Some(value) => value
              case _ => throw new RuntimeException("Cannot understand Data API response.")
            }
          case _ => throw new RuntimeException("Cannot understand Data API response.")
        }

    }
  }

  protected def uriForGrid(grid: GridTile): String = {
    val baseUri = dataEnvironment.getSourceUri(datasetName, grid)
    baseUri.replace("{tile_id}", grid.tileId)
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
        throw new FileNotFoundException(uri)
      }
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
                  windowLayout: LayoutDefinition): OptionalDTile =
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

trait OptionalFLayer extends OptionalLayer with FLayer {

  /**
    * Define how to fetch data for optional double rasters
    */
  def fetchWindow(windowKey: SpatialKey,
                  windowLayout: LayoutDefinition): OptionalFTile =
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
