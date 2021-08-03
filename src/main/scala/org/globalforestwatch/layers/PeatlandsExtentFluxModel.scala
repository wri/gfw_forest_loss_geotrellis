package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class PeatlandsExtentFluxModel(gridTile: GridTile)
  extends BooleanLayer
    with OptionalILayer {
  val uri: String =
//    s"$basePath/gfw_peatlands__flux/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
    s"s3://gfw-files/flux_1_2_0/peatlands_flux_extent/standard/${gridTile.tileId}.tif"
}
