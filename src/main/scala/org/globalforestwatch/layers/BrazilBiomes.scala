package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile
import org.globalforestwatch.config.GfwConfig

case class BrazilBiomes(gridTile: GridTile) extends StringLayer with OptionalILayer {

  val uri: String = uriForGrid(GfwConfig.get.rasterLayers(getClass.getSimpleName()), gridTile)

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
