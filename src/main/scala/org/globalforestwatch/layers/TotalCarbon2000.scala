package org.globalforestwatch.layers

case class TotalCarbon2000(grid: String)
    extends DoubleLayer
    with RequiredDLayer {
  val uri: String = s"$basePath/total_carbon_2000/$grid.tif"
}
