package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class JplTropicsAbovegroundBiomassExtent2000(gridTile: GridTile) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/jpl_tropics_abovegroundbiomass_extent_2000/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
