package org.globalforestwatch.layers

case class TotalCarbonEmisYear(grid: String, model: String="standard")
    extends DoubleLayer
      with OptionalDLayer {
  val uri: String = s"$basePath/total_carbon_emis_year/$model/$grid.tif"
}
