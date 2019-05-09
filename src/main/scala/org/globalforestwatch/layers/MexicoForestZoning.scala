package org.globalforestwatch.layers

class MexicoForestZoning(grid: String) extends StringLayer with OptionalILayer {

  val uri: String =
    s"$basePath/mex_forest_zoning/$grid.tif"

  def lookup(value: Int): String = value match {
    case 1 | 2 | 3 | 4 | 5 | 6 | 7 => "Zonas de conservación y aprovechamiento restringido o prohibido"

    case 8 | 9 | 10 | 11 | 12 | 13 => "Zonas de producción"

    case 14 | 15 | 16 | 17 | 18 => "Zonas de restauración"

    case 19 => "No aplica"

    case _ => null
  }
}
