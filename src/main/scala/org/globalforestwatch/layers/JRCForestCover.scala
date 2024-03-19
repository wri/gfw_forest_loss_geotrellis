package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class JRCForestCover(gridTile: GridTile, kwargs: Map[String, Any])
    extends BooleanLayer
        with OptionalILayer {

    val datasetName = "jrc_global_forest_cover"
    val uri: String = uriForGrid(gridTile, kwargs)
}
