package org.globalforestwatch.layers

case class JplTropicsAbovegroundBiomassDensity2000(grid: String) extends FloatLayer with OptionalFLayer {
  val uri: String = s"$basePath/jpl_tropics_abovegroundbiomass_density_2000/Mg_ha-1/$grid.tif"
}
