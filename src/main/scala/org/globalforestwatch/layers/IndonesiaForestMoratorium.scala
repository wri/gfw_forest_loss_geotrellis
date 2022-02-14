package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class IndonesiaForestMoratorium(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "idn_forest_moratorium"
  val uri: String =
    uriForGrid(gridTile)
}
