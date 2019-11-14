package org.globalforestwatch.layers

case class DeadwoodCarbonEmisYear(grid: String, model: String="standard")
    extends DoubleLayer
      with OptionalDLayer {
  val uri: String = s"$basePath/deadwood_carbon_emis_year/$model/$grid.tif"
}
