package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class SoyPlantedAreas(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "umd_soy_planted_area"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}
