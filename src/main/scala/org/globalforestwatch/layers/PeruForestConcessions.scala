package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class PeruForestConcessions(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "per_forest_concessions"
  val uri: String =
    uriForGrid(gridTile, kwargs)


  override val externalNoDataValue: String = ""

  def lookup(value: Int): String = value match {
    case 1 => "Conservation"
    case 2 => "Ecotourism"
    case 3 => "Timber Concession"
    case 4 => "Nontimber Forest Poducts (Chestnut)"
    case 5 => "Native Community"
    case 6 => "Private Property (Forest Permit)"
    case _ => ""
  }
}
