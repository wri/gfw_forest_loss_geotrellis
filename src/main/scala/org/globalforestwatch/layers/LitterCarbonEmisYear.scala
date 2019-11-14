package org.globalforestwatch.layers

case class LitterCarbonEmisYear(grid: String, model: String="standard")
    extends DoubleLayer
      with OptionalDLayer {
  val uri: String = s"$basePath/litter_carbon_emis_year/$model/$grid.tif"
}
