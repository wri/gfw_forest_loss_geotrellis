package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class PeatlandsExtentFluxModel(gridTile: GridTile)
  extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"$basePath/peatlands_extent_flux_model/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
