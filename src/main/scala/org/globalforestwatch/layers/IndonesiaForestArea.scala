package org.globalforestwatch.layers

case class IndonesiaForestArea(grid: String)
    extends StringLayer
    with OptionalILayer {

  val uri: String =
    s"$basePath/idn_forest_area/$grid.tif"

  def lookup(value: Int): String = value match {
    case 1001        => "Protected Forest"
    case 1003        => "Production Forest"
    case 1004        => "Limited Production Forest"
    case 1005        => "Converted Production Forest"
    case 1007        => "Other Utilization Area"
    case 5001 | 5003 => null
    case 1 | 1002 | 10021 | 10022 | 10023 | 10024 | 10025 | 10026 =>
      "Sanctuary Reserves/Nature Conservation Area"
    case 100201 | 100211 | 100221 | 100241 | 100251 => "Marine Protected Areas"
    case _ => null

  }
}
