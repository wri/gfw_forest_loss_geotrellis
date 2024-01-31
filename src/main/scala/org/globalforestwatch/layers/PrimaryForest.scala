package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class PrimaryForest(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

    val datasetName = "umd_regional_primary_forest_2001"

    val uri: String =
      uriForGrid(gridTile, kwargs)
}
