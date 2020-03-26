package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossAnnualRemovalsBiomass(gridTile: GridTile, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/gross_annual_removals_biomass/$model/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
