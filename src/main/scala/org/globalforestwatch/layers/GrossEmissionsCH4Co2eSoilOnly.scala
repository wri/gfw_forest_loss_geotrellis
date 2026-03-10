package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

//TODO: Update name and s3 path in json to biomass_only. Update here to biomass_only. Refactor this layer's name to biomass_only.
case class GrossEmissionsCH4Co2eSoilOnly(gridTile: GridTile, kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "gfw_forest_flux_full_extent_gross_emissions_ch4_soil_only"

  val uri: String =
    uriForGrid(gridTile, kwargs)

}