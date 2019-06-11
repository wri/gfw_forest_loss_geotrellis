package org.globalforestwatch.layers

case class LitterCarbonEmisYear(grid: String)
    extends DoubleLayer
      with OptionalDLayer {
  val uri: String = s"$basePath/litter_carbon_emis_year/$grid.tif"
}
