package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

// Colombia classification of all its lands into areas where agricultural activity is
// allowed, or is conditional, or is fully restricted. The classification strings are
// intended to be parsed and used directly by the front-end (so front end doesn't
// have to have country-specific information in it).
case class ColFronteraAgricola(gridTile: GridTile, kwargs: Map[String, Any]) extends StringLayer with OptionalILayer {
  val datasetName = "col_frontera_agricola"
  val uri: String = uriForGrid(gridTile, kwargs)

  def lookup(value: Int): String = value match {
    case 1 => "Unconditioned;National Agricultural Frontier"
    case 2 => "Conditioned;Environmental"
    case 3 => "Conditioned;Environmental/Disaster Risk"
    case 4 => "Conditioned;Environmental/Disaster Risk/Ethno-Cultural'"
    case 5 => "Conditioned;Environmental/Ethno-Cultural"
    case 6 => "Conditioned;Disaster Risk Management"
    case 7 => "Conditioned;Disaster Risk/Ethno-Cultural"
    case 8 => "Conditioned;Ethno-Cultural"
    case 9 => "Restrictions;Zero Deforestation Agreement"
    case 10 => "Restrictions;Legal"
    case 11 => "Restrictions;Technical (Non-agricultural areas)"
    case _  => ""
  }
}
