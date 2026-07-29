package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GPWCultivatedGrasslandExtent2023(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "gpw_grassland_extent_2023"

  val uri: String =
    uriForGrid(gridTile, kwargs)
}