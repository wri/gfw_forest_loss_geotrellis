package org.globalforestwatch.treecoverloss

import java.io.FileNotFoundException

import com.typesafe.scalalogging.LazyLogging
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.raster.{MultibandTile, Raster, Tile}
import geotrellis.vector.Extent
import cats.implicits._
import com.amazonaws.services.s3.AmazonS3URI

/**
  * @param grid top left corner, padded from east ex: "10N_010E"
  */
case class TenByTenGridSources(grid: String) extends LazyLogging {
  val lossSourceUri =
    s"s3://wri-users/tmaschler/prep_tiles/loss/${grid}.tif"

  val gainSourceUri =
    s"s3://wri-users/tmaschler/prep_tiles/gain/${grid}.tif"

  val tcd2000SourceUri =
    s"s3://wri-users/tmaschler/prep_tiles/tcd_2000/${grid}.tif"

  val tcd2010SourceUri =
    s"s3://wri-users/tmaschler/prep_tiles/tcd_2010/${grid}.tif"

  val co2PixelSourceUri  =
    s"s3://wri-users/tmaschler/prep_tiles/co2_pixel/${grid}.tif"

  val gadm36SourceUri  =
    s"s3://wri-users/tmaschler/prep_tiles/gadm36/${grid}.tif"

  lazy val lossSource = TenByTenGridSources.requiredSource(lossSourceUri)

  lazy val gainSource = TenByTenGridSources.requiredSource(gainSourceUri)

  lazy val tcd2000Source = TenByTenGridSources.requiredSource(tcd2000SourceUri)

  lazy val tcd2010Source = TenByTenGridSources.requiredSource(tcd2010SourceUri)

  lazy val co2PixelSource: Option[GeoTiffRasterSource] =
    TenByTenGridSources.optionalSource(co2PixelSourceUri)

  lazy val gadm36Source: Option[GeoTiffRasterSource] =
    TenByTenGridSources.optionalSource(gadm36SourceUri)

  def readWindow(window: Extent): Either[Throwable, Raster[TreeLossTile]] = {
    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      loss <- Either.catchNonFatal(lossSource.read(window).get.tile).right
      gain <- Either.catchNonFatal(gainSource.read(window).get.tile).right
      tcd2000 <- Either.catchNonFatal(tcd2000Source.read(window).get.tile).right
      tcd2010 <- Either.catchNonFatal(tcd2010Source.read(window).get.tile).right
    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val co2Pixel: Option[Tile] =
        for {
          source <- co2PixelSource
          raster <- Either.catchNonFatal(source.read(window).get.tile.band(0)).toOption
        } yield raster

      val gadm36: Option[Tile] =
        for {
          source <- gadm36Source
          raster <- {
            println(s"About to read: ${source.uri}")
            Either.catchNonFatal(source.read(window).get.tile.band(0)).toOption
          }
        } yield raster

      val tile = TreeLossTile(
        loss.band(0),
        gain.band(0),
        tcd2000.band(0),
        tcd2010.band(0),
        co2Pixel,
        gadm36)

      Raster(tile, window)
    }
  }
}

object TenByTenGridSources {
  val s3Client = geotrellis.spark.io.s3.S3Client.DEFAULT

  /** Check if URI exists before trying to open it, return None if no file found */
  def optionalSource(uri: String): Option[GeoTiffRasterSource] = {
    // Removes the expected 404 errors from console log
    val s3uri = new AmazonS3URI(uri)
    if (s3Client.doesObjectExist(s3uri.getBucket, s3uri.getKey)) {
      println(s"Opening: $uri")
      Some(GeoTiffRasterSource(uri))
    } else None
  }

  def requiredSource(uri: String): GeoTiffRasterSource = {
    // Removes the expected 404 errors from console log
    val s3uri = new AmazonS3URI(uri)
    if (! s3Client.doesObjectExist(s3uri.getBucket, s3uri.getKey)) {
      throw new FileNotFoundException(uri)
    }

    GeoTiffRasterSource(uri)
  }
}