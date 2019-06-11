package org.globalforestwatch.layers

case class SoilCarbonEmisYear(grid: String)
    extends DoubleLayer
    with RequiredDLayer {
  val uri: String = s"$basePath/soil_carbon_emis_year/$grid.tif"
}
