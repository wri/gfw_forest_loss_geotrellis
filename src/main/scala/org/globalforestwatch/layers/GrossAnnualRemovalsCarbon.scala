package org.globalforestwatch.layers

case class GrossAnnualRemovalsCarbon(grid: String)
    extends DoubleLayer
    with RequiredDLayer {
  val uri: String = s"$basePath/gross_annual_removals_carbon/$grid.tif"
}
