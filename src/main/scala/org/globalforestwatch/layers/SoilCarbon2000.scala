package org.globalforestwatch.layers

case class SoilCarbon2000(grid: String)
    extends DoubleLayer
    with RequiredDLayer {
  val uri: String = s"$basePath/soil_carbon_2000/$grid.tif"
}
