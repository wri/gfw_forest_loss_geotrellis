package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ClimateMask(gridTile: GridTile, kwargs: Map[String, Any]) extends BooleanLayer with OptionalILayer {
  // TODO: will this be in data lake?

  val datasetName = "Na"


  // This path is incorrect, should really be:
  // "s3://gfw2-data/forest_change/umd_landsat_alerts/archive/pipeline/climate/climate_mask/${gridTile.tileId}.tif"
  // But no analysis actually uses this layer anyways.
  val uri: String =
    s"s3://gfw2-data/forest_change/umd_landsat_alerts/archive/pipeline/climate/climate_mask/climate_mask_geotiff/${gridTile.tileId}.tif"
}
