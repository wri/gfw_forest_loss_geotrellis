package org.globalforestwatch.layers

case class PeruForestConcessions(grid: String)
    extends StringLayer
    with OptionalILayer {

  val uri: String =
    s"$basePath/per_forest_concessions/$grid.tif"

  override val externalNoDataValue: String = ""

  def lookup(value: Int): String = value match {
    case 1 => "Conservation"
    case 2 => "Ecotourism"
    case 3 => "Nontimber Forest Poducts (Nuts)"
    case 4 => "Nontimber Forest Poducts (Shiringa)"
    case 5 => "Reforestation"
    case 6 => "Timber Concession"
    case 7 => "Wildlife"
    case _ => ""
  }
}
