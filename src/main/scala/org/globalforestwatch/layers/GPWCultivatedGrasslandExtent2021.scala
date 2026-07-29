package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GPWCultivatedGrasslandExtent2021(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "gpw_grassland_extent_2021"

  val uri: String =
    uriForGrid(gridTile, kwargs)
}