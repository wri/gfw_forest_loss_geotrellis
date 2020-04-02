package org.globalforestwatch.layers

// For Mars analysis using Mapspam grid, where grid cell ID must be maintained through analysis.
// Based on tree cover loss layer.
case class CarbonFluxCustomArea1(grid: String) extends IntegerLayer with OptionalILayer {
  val uri: String = s"$basePath/carbon_flux_custom_area_1/$grid.tif"

  override def lookup(value: Int): Integer = if (value < 0) null else value
}
