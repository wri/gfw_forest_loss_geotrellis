package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class AbovegroundBiomass2000(gridTile: GridTile, kwargs: Map[String, Any]) extends DoubleLayer with OptionalDLayer {
  val datasetName = "whrc_aboveground_biomass_density_2000"
  val uri: String = uriForGrid(gridTile, kwargs)
}
