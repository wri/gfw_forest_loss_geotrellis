package org.globalforestwatch.treecoverloss

import com.typesafe.scalalogging.LazyLogging
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.raster.{MultibandTile, Raster, Tile}
import geotrellis.vector.Extent
import cats.implicits._

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
    s"s3://wri-users/tmaschler/prep_tiles/co2_pixel/${grid}.tif"

  lazy val lossSource = GeoTiffRasterSource(lossSourceUri)

  lazy val gainSource = GeoTiffRasterSource(gainSourceUri)

  lazy val tcd2000Source = GeoTiffRasterSource(tcd2000SourceUri)

  lazy val tcd2010Source = GeoTiffRasterSource(tcd2010SourceUri)

  lazy val co2PixelSource = GeoTiffRasterSource(co2PixelSourceUri)

  lazy val gadm36Source = GeoTiffRasterSource(gadm36SourceUri)

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
        Either.catchNonFatal(co2PixelSource.read(window).get.tile.band(0)).toOption

      val gadm36: Option[Tile] =
        Either.catchNonFatal(gadm36Source.read(window).get.tile.band(0)).toOption

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
