package org.globalforestwatch.layers

import java.io.FileNotFoundException
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.raster.{CellType, Tile, isNoData}
import geotrellis.vector.Extent
import cats.implicits._
import com.amazonaws.services.s3.AmazonS3URI

trait Layer {

  /**
    * Layers include information on how/ where to access specific raster data and how to interpret the pixel values
    */
  type A
  type B

  val s3Client: geotrellis.spark.io.s3.S3Client =
    geotrellis.spark.io.s3.S3Client.DEFAULT
  val uri: String
  def noDataValue: B
  def lookup(a: A): B

  val basePath: String = s"s3://gfw-files/2018_update"
}

trait ILayer extends Layer {

  /**
    * Wrapper for interger rasters to assure that data according to layer specifications are returned
    */
  type A = Int

  implicit class ITile(val t: Tile) {
    def getData(col: Int, row: Int): B = {
      val value: Int = t.get(col, row)
      if (isNoData(value)) noDataValue else lookup(value)
    }
    def cellType: CellType = t.cellType
    def cols: Int = t.cols
    def rows: Int = t.rows
    def noDataValue: B = noDataValue
  }

  implicit class OptionalITile(val t: Option[Tile]) {
    def getData(col: Int, row: Int): B = {
      val value: Int = t.map(_.get(col, row)).getOrElse(noDataValue)
      if (isNoData(value) || value == noDataValue) noDataValue
      else lookup(value)
    }

    def cellType: CellType = t.cellType
    def cols: Int = t.cols
    def rows: Int = t.rows
    def noDataValue: B = noDataValue
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
      if (isNoData(value)) noDataValue else lookup(value)
    }
    def cellType: CellType = t.cellType
    def cols: Int = t.cols
    def rows: Int = t.rows
    def noDataValue: B = noDataValue
  }

  implicit class OptionalDTile(val t: Option[Tile]) {
    def getData(col: Int, row: Int): B = {
      val value: Double = t.map(_.getDouble(col, row)).getOrElse(noDataValue)
      if (isNoData(value) || value == noDataValue) noDataValue
      else lookup(value)
    }
    def cellType: CellType = t.cellType
    def cols: Int = t.cols
    def rows: Int = t.rows
    def noDataValue: B = noDataValue
  }

}

trait RequiredLayer extends Layer {

  /**
    * Define how to read sources for required layers
    */
  lazy val source: GeoTiffRasterSource = fetchSource

  def fetchSource: GeoTiffRasterSource = {
    // Removes the expected 404 errors from console log
    val s3uri = new AmazonS3URI(uri)
    if (!s3Client.doesObjectExist(s3uri.getBucket, s3uri.getKey)) {
      throw new FileNotFoundException(uri)
    }
    GeoTiffRasterSource(uri)
  }

}


trait RequiredILayer extends RequiredLayer with ILayer {

  /**
    * Define how to fetch data for required Integer rasters
    */

  def fetchWindow(window: Extent): ITile =
    new ITile(source.read(window).get.tile.band(0))

}

trait RequiredDLayer extends RequiredLayer with DLayer {

  /**
    * Define how to fetch data for required Double rasters
    */

  def fetchWindow(window: Extent): DTile =
    new DTile(source.read(window).get.tile.band(0))

}

trait OptionalLayer extends Layer {

  /**
    * Define how to read sources for optional Layers
    */
  lazy val source: Option[GeoTiffRasterSource] = fetchSource

  /** Check if URI exists before trying to open it, return None if no file found */
  def fetchSource: Option[GeoTiffRasterSource] = {
    // Removes the expected 404 errors from console log
    val s3uri = new AmazonS3URI(uri)
    if (s3Client.doesObjectExist(s3uri.getBucket, s3uri.getKey)) {
      println(s"Opening: $uri")
      Some(GeoTiffRasterSource(uri))
    } else None
  }

}

trait OptionalILayer extends OptionalLayer with ILayer {

  /**
    * Define how to fetch data for optional Integer rasters
    */

  def fetchWindow(window: Extent): OptionalITile =
    new OptionalITile(for {
      source <- source
      raster <- Either
        .catchNonFatal(source.read(window).get.tile.band(0))
        .toOption
    } yield raster)
}

trait OptionalDLayer extends OptionalLayer with DLayer {

  /**
    * Define how to fetch data for optional double rasters
    */

  def fetchWindow(window: Extent): OptionalDTile = new OptionalDTile(for {
    source <- source
    raster <- Either
      .catchNonFatal(source.read(window).get.tile.band(0))
      .toOption
  } yield raster)
}

trait BooleanLayer extends ILayer {

  /**
    * Layers which return a Boolean type
    */
  type B = Boolean
  def noDataValue: Boolean = false
  def lookup(value: Int): Boolean = if (value == 0) false else true
}

trait IntegerLayer extends ILayer {

  /**
    * Layers which return an Integer type
    * (use java.lang.Integer to be able to use null)
    */
  type B = Integer
  def noDataValue: Integer = null
  def lookup(value: Int): Integer = value
}

trait DoubleLayer extends DLayer {

  /**
    * Layers which return a Double type
    */
  type B = Double
  def noDataValue: Double = 0.0
  def lookup(value: Double): Double = value
}

trait StringLayer extends ILayer {

  /**
    * Layers which return a String type
    */
  type B = String
  def noDataValue: String = null
  def lookup(value: Int): String
}
