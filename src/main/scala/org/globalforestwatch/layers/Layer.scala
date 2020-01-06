package org.globalforestwatch.layers

import java.io.FileNotFoundException

import cats.implicits._
import com.amazonaws.services.s3.AmazonS3URI
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.raster.crop._
import geotrellis.raster.{CellType, Tile, isNoData}
import geotrellis.vector.Extent

trait Layer {

  /**
    * Layers include information on how/ where to access specific raster data and how to interpret the pixel values
    */
  type A
  type B

  val s3Client: geotrellis.spark.io.s3.S3Client =
    geotrellis.spark.io.s3.S3Client.DEFAULT
  val uri: String
  val internalNoDataValue: A
  val externalNoDataValue: B
  val basePath: String = s"s3://gfw-files/2018_update"

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
    def cellType: CellType = t.cellType
    def cols: Int = t.cols
    def rows: Int = t.rows

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
  lazy val source: GeoTiffRasterSource = {
    // Removes the expected 404 errors from console log
    val s3uri = new AmazonS3URI(uri)
    if (!s3Client.doesObjectExist(s3uri.getBucket, s3uri.getKey)) {
      throw new FileNotFoundException(uri)
    }
    GeoTiffRasterSource(uri)
  }

  //  lazy val extent: Extent = {
  //    source.extent
  //  }

  def cropWindow(tile: Tile): Tile = {

    // TODO: don't use magic number 401. Instead fetch number from 10x10 grid block definition

    val cols = tile.cols
    val rows = tile.rows

    if (cols == 401 && rows == 401)
      tile.crop(1, 1, cols, rows, Crop.Options(force = true))
    else if (cols == 401)
      tile.crop(1, 0, cols, rows, Crop.Options(force = true))
    else if (rows == 401)
      tile.crop(0, 1, cols, rows, Crop.Options(force = true))
    else tile

  }

}

trait RequiredILayer extends RequiredLayer with ILayer {

  /**
    * Define how to fetch data for required Integer rasters
    */
  def fetchWindow(window: Extent): ITile = {
    val tile = source.synchronized {
      source.read(window).get.tile.band(0)
    }
    new ITile(cropWindow(tile))
  }

}

trait RequiredDLayer extends RequiredLayer with DLayer {

  /**
    * Define how to fetch data for required Double rasters
    */
  def fetchWindow(window: Extent): DTile = {
    val tile = source.synchronized {
      source.read(window).get.tile.band(0)
    }
    new DTile(cropWindow(tile))
  }

}


trait RequiredFLayer extends RequiredLayer with FLayer {

  /**
    * Define how to fetch data for required Double rasters
    */
  def fetchWindow(window: Extent): FTile = {
    val tile = source.synchronized {
      source.read(window).get.tile.band(0)
    }
    new FTile(cropWindow(tile))
  }

}

trait OptionalLayer extends Layer {

  /**
    * Define how to read sources for optional Layers
    */
  /** Check if URI exists before trying to open it, return None if no file found */
  lazy val source: Option[GeoTiffRasterSource] = {
    // Removes the expected 404 errors from console log
    val s3uri = new AmazonS3URI(uri)
    if (s3Client.doesObjectExist(s3uri.getBucket, s3uri.getKey)) {
      println(s"Opening: $uri")
      Some(GeoTiffRasterSource(uri))
    } else {
      println(s"Cannot open: $uri")
      None
    }
  }

  //  lazy val extent: Option[Extent] = source match {
  //
  //    case Some(s) => Some(s.extent)
  //    case None => None
  //
  //  }

  def cropWindow(tile: Option[Tile]): Option[Tile] = {

    // TODO: don't use magic number 401. Instead fetch number from 10x10 grid block definition

    tile match {
      case Some(tile) => {
        val cols = tile.cols
        val rows = tile.rows

        if (cols == 401 && rows == 401)
          Option(tile.crop(1, 1, cols, rows, Crop.Options(force = true)))
        else if (cols == 401)
          Option(tile.crop(1, 0, cols, rows, Crop.Options(force = true)))
        else if (rows == 401)
          Option(tile.crop(0, 1, cols, rows, Crop.Options(force = true)))
        else Option(tile)
      }
      case None => tile
    }

  }
}

trait OptionalILayer extends OptionalLayer with ILayer {

  /**
    * Define how to fetch data for optional Integer rasters
    */
  def fetchWindow(window: Extent): OptionalITile = {

    new OptionalITile(cropWindow(for {
      source <- source
      raster <- Either
        .catchNonFatal(source.synchronized {
          source.read(window).get.tile.band(0)
        })
        .toOption
    } yield raster))
  }
}

trait OptionalDLayer extends OptionalLayer with DLayer {

  /**
    * Define how to fetch data for optional double rasters
    */
  def fetchWindow(window: Extent): OptionalDTile =
    new OptionalDTile(cropWindow(for {
      source <- source
      raster <- Either
        .catchNonFatal(source.synchronized {
          source.read(window).get.tile.band(0)
        })
        .toOption
    } yield raster))
}

trait OptionalFLayer extends OptionalLayer with FLayer {

  /**
    * Define how to fetch data for optional double rasters
    */
  def fetchWindow(window: Extent): OptionalFTile =
    new OptionalFTile(cropWindow(for {
      source <- source
      raster <- Either
        .catchNonFatal(source.synchronized {
          source.read(window).get.tile.band(0)
        })
        .toOption
    } yield raster))
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

  /**
    * Layers which return an Integer type
    * (use java.lang.Integer to be able to use null)
    */
  type B = Option[(String, Boolean)]

  val internalNoDataValue: Int = 0
  val externalNoDataValue: B = None

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
