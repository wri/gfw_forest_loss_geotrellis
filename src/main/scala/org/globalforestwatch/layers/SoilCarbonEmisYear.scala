package org.globalforestwatch.layers

case class SoilCarbonEmisYear(grid: String, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/soil_carbon_emis_year/$model/$grid.tif"
}
