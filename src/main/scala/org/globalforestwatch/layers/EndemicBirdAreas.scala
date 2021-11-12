package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class EndemicBirdAreas(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "birdlife_endemic_bird_areas"
  val uri: String =
    uriForGrid(gridTile)
}
