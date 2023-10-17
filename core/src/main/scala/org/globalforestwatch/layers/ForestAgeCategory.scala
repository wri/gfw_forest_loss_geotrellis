package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ForestAgeCategory(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "gfw_forest_age"
  val uri: String = uriForGrid(gridTile, kwargs)

  override val externalNoDataValue = "Not applicable"

  def lookup(value: Int): String = value match {
    case 1 => "Secondary forest <=20 years"
    case 2  => "Secondary forest >20 years"
    case 3  => "Primary forest"
    case _ => "Unknown"
  }
}
