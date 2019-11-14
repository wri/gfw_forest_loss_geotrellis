package org.globalforestwatch.layers

case class GrossAnnualRemovalsBiomass(grid: String, model: String="standard")
    extends DoubleLayer
      with OptionalDLayer {
  val uri: String = s"$basePath/gross_annual_removals_biomass/$model/$grid.tif"
}
