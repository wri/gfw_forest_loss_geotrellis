package org.globalforestwatch.treecoverloss

import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource

/**
  * @param grid top left corner, padded from east ex: "10N_010E"
  */
case class TenByTenGridSources(grid: String) {
  val forestChangeSourceUri =
    s"s3://gfw2-data/forest_change/hansen_2018/${grid}.tif"

  val treeCoverSourceUri =
    s"s3://gfw2-data/forest_cover/2000_treecover/Hansen_GFC2014_treecover2000_${grid}.tif"

  val bioMassSourceUri  =
    s"s3://gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/${grid}_t_aboveground_biomass_ha_2000.tif"

  lazy val forestChangeSource = GeoTiffRasterSource(forestChangeSourceUri)

  lazy val treeCoverSource = GeoTiffRasterSource(treeCoverSourceUri)

  lazy val bioMassSource = GeoTiffRasterSource(bioMassSourceUri)
}
