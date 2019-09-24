package org.globalforestwatch.layers

case class GrossEmissionsCo2OnlyCo2e(grid: String)
    extends DoubleLayer
    with OptionalDLayer {
  val uri: String = s"$basePath/gross_emissions_co2_only_co2e/$grid.tif"
}
