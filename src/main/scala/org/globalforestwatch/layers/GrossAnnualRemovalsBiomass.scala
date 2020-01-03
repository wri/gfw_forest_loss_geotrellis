package org.globalforestwatch.layers

case class GrossAnnualRemovalsBiomass(grid: String)
    extends DoubleLayer
      with OptionalDLayer {
  val uri: String = s"$basePath/gross_annual_removals_biomass/$grid.tif"
}
