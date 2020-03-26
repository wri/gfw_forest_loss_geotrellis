package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossEmissionsNonCo2Co2e(gridTile: GridTile, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/gross_emissions_non_co2_co2e/$model/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
