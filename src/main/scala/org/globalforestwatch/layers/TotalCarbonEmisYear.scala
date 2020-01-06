package org.globalforestwatch.layers

case class TotalCarbonEmisYear(grid: String)
    extends DoubleLayer
      with OptionalDLayer {
  val uri: String = s"$basePath/total_carbon_emis_year/$grid.tif"
}
