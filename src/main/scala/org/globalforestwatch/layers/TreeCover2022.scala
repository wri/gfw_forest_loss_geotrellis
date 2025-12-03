package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

// tree_cover_2022 is defined as:
//    
//    (umd_tree_cover_density_2010/v1.6 >= 30) and
//    (umd_tree_cover_height_2020/v2022 >= 3) and
//    (umd_tree_cover_loss/v1.12 != 2021)
//    
//    It is the tree cover filter that we use as an option for dist alerts and
//    (soon) integrated-dist alerts.

case class TreeCover2022(gridTile: GridTile, kwargs: Map[String, Any])
    extends BooleanLayer
        with OptionalILayer {

    val datasetName = "tree_cover_2022"
    val uri: String = uriForGrid(gridTile, kwargs)
}
