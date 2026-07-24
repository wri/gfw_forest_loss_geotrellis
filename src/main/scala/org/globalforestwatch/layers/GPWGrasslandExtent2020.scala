package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GPWGrasslandExtent2020(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "gpw_grassland_extent_2020"

  val uri: String =
    uriForGrid(gridTile, kwargs)

  override val internalNoDataValue = 255
  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 1 => "Cultivated"
    case 2 => "Natural or Semi-Natural"
    case _ => "Unknown"
  }
}