package org.globalforestwatch.layers

case class GrossEmissionsNonCo2Co2e(grid: String)
    extends DoubleLayer
    with OptionalDLayer {
  val uri: String = s"$basePath/gross_emissions_non_co2_co2e/$grid.tif"
}
