package org.globalforestwatch.layers

case class DeadwoodCarbonEmisYear(grid: String)
    extends DoubleLayer
    with RequiredDLayer {
  val uri: String = s"$basePath/deadwood_carbon_emis_year/$grid.tif"
}
