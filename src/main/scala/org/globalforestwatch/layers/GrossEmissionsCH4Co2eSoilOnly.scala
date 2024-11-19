package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossEmissionsCH4Co2eSoilOnly(gridTile: GridTile, kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "gfw_forest_flux_full_extent_gross_emissions_ch4_soil_only"

  val uri: String =
    uriForGrid(gridTile, kwargs)

}