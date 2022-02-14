package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ResourceRights(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "gfw_resource_rights"
  val uri: String =
    uriForGrid(gridTile)
}
