package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class PeruProtectedAreas(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "per_protected_areas"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}
