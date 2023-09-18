package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class DetailedProtectedAreas(gridTile: GridTile, kwargs: Map[String, Any]) extends StringLayer with OptionalILayer {
  val datasetName = "detailed_wdpa_protected_areas"
  val uri: String = uriForGrid(gridTile, kwargs)

  def lookup(value: Int): String = value match {
    case 1 => "Category Ia"
    case 2 => "Category Ib"
    case 3 => "Category II"
    case 4 => "Category III"
    case 5 => "Category IV"
    case 6 => "Category V"
    case 7 => "Category VI"
    case 8 => "UNESCO-MAB Biosphere Reserve"
    case 9 => "World Heritage Site (natural or mixed)"
    case 10 => "Ramsar Site, Wetland of International Importance"
    case 11 => "Not Reported"
    case 12 => "Not Applicable"
    case 13 => "Not Assigned"
    case _  => "Unknown"
  }
}
