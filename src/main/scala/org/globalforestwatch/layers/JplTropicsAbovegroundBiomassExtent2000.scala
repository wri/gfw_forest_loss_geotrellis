package org.globalforestwatch.layers

case class JplTropicsAbovegroundBiomassExtent2000(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/jpl_tropics_abovegroundbiomass_extent_2000/$grid.tif"
}
