package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class JplTropicsAbovegroundBiomassExtent2000(gridTile: GridTile)
  extends BooleanLayer
    with OptionalILayer {

  val uri: String =
//    s"$basePath/jpl_tropics_abovegroundbiomass_2000/v20191106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/extent/geotiff/${gridTile.tileId}.tif"
    s"s3://gfw-files/2018_update/jpl_tropics_abovegroundbiomass_extent_2000/${gridTile.tileId}.tif"
}
