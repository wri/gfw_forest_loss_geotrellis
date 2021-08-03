package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class MekongTreeCoverLossExtent(gridTile: GridTile)
  extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"$basePath/Mekong_loss_extent/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}

