package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ArgForestLoss(gridTile: GridTile, kwargs: Map[String, Any])
  extends IntegerLayer
    with OptionalILayer {
  val datasetName = "arg_otbn_forest_loss"
  val uri: String =
    uriForGrid(gridTile, kwargs)

  override def lookup(value: Int): Integer =
    if (value == 0) null else value + 2000
}
