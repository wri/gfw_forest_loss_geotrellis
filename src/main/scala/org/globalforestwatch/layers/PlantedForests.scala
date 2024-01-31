package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class PlantedForests(gridTile: GridTile, kwargs: Map[String, Any]) extends StringLayer with OptionalILayer {
  val datasetName = "gfw_planted_forests"

  val uri: String = uriForGrid(gridTile, kwargs)

  def lookup(value: Int): String = value match {
    case 1 => "Fruit"
    case 2 => "Fruit mix"
    case 3 => "Oil palm "
    case 4 => "Oil palm mix"
    case 5 => "Other"
    case 6 => "Other mix"
    case 7 => "Rubber"
    case 8  => "Rubber mix"
    case 9  => "Unknown"
    case 10  => "Unknown mix"
    case 11 => "Wood fiber or timber"
    case 12 => "Wood fiber or timber Mix"
    case _ => ""
  }
}

case class PlantedForestsBool(gridTile: GridTile, kwargs: Map[String, Any]) extends BooleanLayer with OptionalILayer {
  val datasetName = "gfw_planted_forests"

  val uri: String = uriForGrid(gridTile, kwargs)
}
