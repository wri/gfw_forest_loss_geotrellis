package org.globalforestwatch.layers

case class GrossAnnualRemovalsCarbon(grid: String)
    extends DoubleLayer
      with OptionalDLayer {
  val uri: String = s"$basePath/gross_annual_removals_carbon/$grid.tif"
}
