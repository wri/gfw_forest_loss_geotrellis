package org.globalforestwatch.layers

case class DeadwoodCarbonEmisYear(grid: String, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/deadwood_carbon_emis_year/$model/$grid.tif"
}
