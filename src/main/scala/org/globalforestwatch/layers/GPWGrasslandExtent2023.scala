package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GPWGrasslandExtent2023(gridTile: GridTile, kwargs: Map[String, Any])
  extends IntLayer
    with OptionalILayer {

  val datasetName = "gpw_grassland_extent_2023"

  val uri: String =
    uriForGrid(gridTile, kwargs)
}