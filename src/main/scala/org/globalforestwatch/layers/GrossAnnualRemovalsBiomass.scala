package org.globalforestwatch.layers

case class GrossAnnualRemovalsBiomass(grid: String, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/gross_annual_removals_biomass/$model/$grid.tif"
}
