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
    case 1 => "No condicionado;Frontera agricola nacional"
    case 2 => "Condicionado;Ambiental"
    case 3 => "Condicionado;Ambiental/Riesgo de desastres"
    case 4 => "Condicionado;Ambiental/Riesgo de desastres/Étnico-Cultural'"
    case 5 => "Condicionado;Ambiental/Étnico-Cultural"
    case 6 => "Condicionado;Gestión riesgo de desastres"
    case 7 => "Condicionado;Riesgo de desastres/Étnico-Cultural"
    case 8 => "Condicionado;Étnico-Cultural"
    case 9 => "Zona restricción;Restricciones acuerdo cero deforestación"
    case 10 => "Zona restricción;Restricciones legales"
    case 11 => "Zona restricción;Restricciones técnicas (Áreas no agropecuarias)"
    case _  => ""
  }
}
