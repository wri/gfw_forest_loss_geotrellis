package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreeCoverLossDrivers1km(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "wri_google_tree_cover_loss_drivers"
  val uri: String =
    uriForGrid(gridTile, kwargs)

  override val internalNoDataValue = 0
  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 1 => "Permanent agriculture"
    case 2 => "Hard commodities"
    case 3 => "Shifting cultivation"
    case 4 => "Logging"
    case 5 => "Wildfire"
    case 6 => "Settlements & Infrastructure"
    case 7 => "Other natural disturbances"
    case _ => "Unknown"
  }
}