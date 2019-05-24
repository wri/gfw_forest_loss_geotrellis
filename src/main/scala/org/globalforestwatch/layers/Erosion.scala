package org.globalforestwatch.layers

case class Erosion(grid: String) extends StringLayer with OptionalILayer {
  val uri: String = s"$basePath/erosion/$grid.tif"
  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 1 => "Low risk"
    case 2 => "Low to medium risk"
    case 3 => "Medium to high risk"
    case 4 => "High risk"
    case 5 => "Extremely high risk"
    case _ => "Unknown"
  }
}