package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class PlantedForests(gridTile: GridTile, kwargs: Map[String, Any]) extends StringLayer with OptionalILayer {
  val datasetName = "gfw_planted_forests"

  val uri: String = uriForGrid(gridTile, kwargs)

  def lookup(value: Int): String = value match {
    case 1 => "Fruit"
    case 2 => "Fruit Mix"
    case 3 => "Oil Palm "
    case 4 => "Oil Palm Mix"
    case 5 => "Other"
    case 6 => "Rubber"
    case 7  => "Rubber Mix"
    case 8  => "Unknown"
    case 9  => "Unknown Mix"
    case 10 => "Wood fiber / Timber"
    case 11 => "Wood fiber / Timber Mix"
    case _ => ""
  }
}

case class PlantedForestsBool(gridTile: GridTile, kwargs: Map[String, Any]) extends BooleanLayer with OptionalILayer {
  val datasetName = "gfw_planted_forests"

  val uri: String = uriForGrid(gridTile, kwargs)
}
