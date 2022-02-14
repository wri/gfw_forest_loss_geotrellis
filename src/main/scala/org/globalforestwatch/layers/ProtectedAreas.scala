package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile
import org.globalforestwatch.config.GfwConfig

case class ProtectedAreas(gridTile: GridTile, kwargs: Map[String, Any]) extends StringLayer with OptionalILayer {
  val datasetName = "wdpa_protected_areas"
  val uri: String = uriForGrid(gridTile)

  def lookup(value: Int): String = value match {
    case 1 => "Category Ia/b or II"
    case 2 => "Other Category"
    case _ => ""
  }
}
