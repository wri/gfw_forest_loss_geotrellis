package org.globalforestwatch.layers

case class GrossEmissionsNonCo2Co2e(grid: String, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/gross_emissions_non_co2_co2e/$model/$grid.tif"
}
