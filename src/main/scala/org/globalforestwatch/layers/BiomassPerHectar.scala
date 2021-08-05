package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class BiomassPerHectar(gridTile: GridTile, kwargs: Map[String, Any]) extends DoubleLayer with OptionalDLayer {
  val datasetName = "whrc_aboveground_biomass_stock_2000"
  val uri: String = s"$basePath/$datasetName/$version/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/Mg_ha-1/gdal-geotiff/${gridTile.tileId}.tif"
}
