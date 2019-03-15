package org.globalforestwatch.treecoverloss

import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource

/**
  * @param grid top left corner, padded from east ex: "10N_010E"
  */
case class TenByTenGridSources(grid: String) {
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

}
