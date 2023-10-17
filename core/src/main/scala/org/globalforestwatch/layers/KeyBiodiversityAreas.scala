package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class KeyBiodiversityAreas(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "birdlife_key_biodiversity_areas"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}
