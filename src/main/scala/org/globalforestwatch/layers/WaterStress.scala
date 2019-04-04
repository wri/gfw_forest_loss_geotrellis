package org.globalforestwatch.layers

class WaterStress(grid: String) extends StringLayer with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/water_stress/${grid}.tif"

  def lookup(value: Int): String = value match {
    case 1 => "Low risk"
    case 2 => "Low to medium risk"
    case 3 => "Medium to high risk"
    case 4 => "High risk"
    case 5 => "Extremely high risk"
  }
}
