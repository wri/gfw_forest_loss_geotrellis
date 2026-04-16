package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossEmissionsNonCo2Co2eBiomassOnly(gridTile: GridTile, kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "gfw_forest_flux_full_extent_gross_emissions_non_co2_biomass_only"

  val uri: String =
    uriForGrid(gridTile, kwargs)

}
