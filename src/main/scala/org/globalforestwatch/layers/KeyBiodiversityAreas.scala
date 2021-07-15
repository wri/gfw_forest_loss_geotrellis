package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class KeyBiodiversityAreas(gridTile: GridTile)
    extends BooleanLayer
    with OptionalILayer {
    val uri: String = s"$basePath/birdlife_key_biodiversity_areas/v202106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
}
