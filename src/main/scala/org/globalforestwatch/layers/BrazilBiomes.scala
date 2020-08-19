package org.globalforestwatch.layers

case class BrazilBiomes(grid: String) extends StringLayer with OptionalILayer {

  val uri: String = s"$basePath/bra_biomes/$grid.tif"

  def lookup(value: Int): String = value match {
    case 1 => "Caatinga"
    case 2 => "Cerrado"
    case 3 => "Pantanal"
    case 4 => "Pampa"
    case 5 => "Amazônia"
    case 6 => "Mata Atlântica"
    case _ => ""
  }

}
