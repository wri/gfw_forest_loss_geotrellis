package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreeCoverLossDrivers(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "tsc_tree_cover_loss_drivers"
  val uri: String =
    uriForGrid(gridTile, kwargs)

  override val internalNoDataValue = 0
  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 1 => "Commodity driven deforestation"
    case 2 => "Shifting agriculture"
    case 3 => "Forestry"
    case 4 => "Wildfire"
    case 5 => "Urbanization"
    case _ => "Unknown"
  }
}
