package org.globalforestwatch.layers

case class TotalCarbonEmisYear(grid: String, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/total_carbon_emis_year/$model/$grid.tif"
}
