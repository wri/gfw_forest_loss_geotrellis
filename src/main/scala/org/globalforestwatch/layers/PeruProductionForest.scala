package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class PeruProductionForest(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "osinfor_peru_permanent_production_forests"
  val uri: String =
    uriForGrid(gridTile)
}
