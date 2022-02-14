package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class RSPO(gridTile: GridTile, kwargs: Map[String, Any]) extends StringLayer with OptionalILayer {

  val datasetName = "rspo_oil_palm"
  val uri: String = uriForGrid(gridTile)

  def lookup(value: Int): String = value match {
    case 1 => "Certified"
    case 2 => "Unknown"
    case 3 => "Not certified"
    case _ => ""
  }
}
