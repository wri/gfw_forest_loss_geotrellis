package org.globalforestwatch.layers

case class GrossEmissionsCo2eCo2Only(grid: String)
    extends DoubleLayer
    with OptionalDLayer {
  val uri: String = s"$basePath/gross_emissions_co2_gas_co2e/$grid.tif"
}
