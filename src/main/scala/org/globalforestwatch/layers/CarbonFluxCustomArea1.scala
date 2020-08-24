package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

// For Mars analysis using Mapspam grid, where grid cell ID must be maintained through analysis.
// Based on tree cover loss layer.
case class CarbonFluxCustomArea1(gridTile: GridTile) extends IntegerLayer with OptionalILayer {
  val uri: String = s"$basePath/carbon_flux_custom_area_1/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"

  override def lookup(value: Int): Integer = if (value < 0) null else value
}
