package org.globalforestwatch.layers

case class GrossEmissionsCo2eNoneCo2(grid: String)
    extends DoubleLayer
    with OptionalDLayer {
  val uri: String = s"$basePath/gross_emissions_non_co2_gas_co2e/$grid.tif"
}
