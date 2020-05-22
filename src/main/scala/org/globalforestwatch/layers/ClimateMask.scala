package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ClimateMask(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  // TODO: will this be in data lake?
  val uri: String =
    s"s3://gfw2-data/forest_change/umd_landsat_alerts/archive/pipeline/climate/climate_mask/climate_mask_geotiff/${gridTile.tileId}.tif"
}
