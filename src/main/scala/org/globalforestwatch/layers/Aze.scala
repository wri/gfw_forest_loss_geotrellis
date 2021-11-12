package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Aze(gridTile: GridTile, kwargs: Map[String, Any]) extends BooleanLayer with OptionalILayer {

  val datasetName = "birdlife_alliance_for_zero_extinction_sites"
  val uri: String = uriForGrid(gridTile)
}


