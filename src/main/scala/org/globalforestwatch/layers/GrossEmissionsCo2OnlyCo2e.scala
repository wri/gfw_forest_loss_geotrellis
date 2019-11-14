package org.globalforestwatch.layers

case class GrossEmissionsCo2OnlyCo2e(grid: String, model: String="standard")
    extends DoubleLayer
      with OptionalDLayer {
  val uri: String = s"$basePath/gross_emissions_co2_only_co2e/$model/$grid.tif"
}
