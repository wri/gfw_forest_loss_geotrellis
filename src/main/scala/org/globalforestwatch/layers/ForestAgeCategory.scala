package org.globalforestwatch.layers

case class ForestAgeCategory(grid: String, model: String="standard") extends StringLayer with OptionalILayer {

  val uri: String = s"$basePath/forest_age_category/standard/$grid.tif"
  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 0  => "No age assigned"
    case 1  => "Secondary forest >20 years"
    case 2  => "Secondary forest >20 years"
    case 3  => "Primary forest"
    case 4  => "Secondary forest <=20 years"
    case 5  => "Secondary forest >20 years"
    case 6  => "Primary forest"
    case 7  => "Secondary forest <=20 years"
    case 8  => "Secondary forest <=20 years"
    case 9  => "Secondary forest <=20 years"
    case 10 => "Secondary forest <=20 years"
    case _ => "No age assigned"
  }
}
