package org.globalforestwatch.layers

case class SoilCarbonEmisYear(grid: String, model: String="standard")
    extends DoubleLayer
      with OptionalDLayer {
  val uri: String = s"$basePath/soil_carbon_emis_year/$model/$grid.tif"
}
