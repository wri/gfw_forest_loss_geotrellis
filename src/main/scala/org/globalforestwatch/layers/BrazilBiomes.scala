package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile
import org.globalforestwatch.config.GfwConfig

case class BrazilBiomes(gridTile: GridTile, kwargs: Map[String, Any]) extends StringLayer with OptionalILayer {

  val datasetName = "ibge_bra_biomes"

  val uri: String =
    uriForGrid(gridTile, kwargs)

  override val externalNoDataValue = "Not applicable"

  def lookup(value: Int): String = value match {
    case 1 => "Caatinga"
    case 2 => "Cerrado"
    case 3 => "Pantanal"
    case 4 => "Pampa"
    case 5 => "Amazônia"
    case 6 => "Mata Atlântica"
    case _ => "Unknown"
  }
}
