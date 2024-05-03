package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreeCoverGain(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {
  val datasetName = "umd_tree_cover_gain"
  val uri: String =
    uriForGrid(gridTile, kwargs)

  override val internalNoDataValue: Int = 0
  override val externalNoDataValue: String = ""

  def lookup(value: Int): String = value match {
    case 1 => "2000-2005"
    case 2 => "2005-2010"
    case 3 => "2010-2015"
    case 4 => "2015-2020"
  }
}