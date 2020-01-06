package org.globalforestwatch.layers

case class LitterCarbonEmisYear(grid: String, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/litter_carbon_emis_year/$model/$grid.tif"
}
