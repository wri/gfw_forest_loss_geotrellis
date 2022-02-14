package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Landmark(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "landmark_indigenous_and_community_lands"
  val uri: String =
    uriForGrid(gridTile)
}
