package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossEmissionsN2OCo2eSoilOnly(gridTile: GridTile, kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "gfw_forest_flux_full_extent_gross_emissions_n2o_soil_only"

  val uri: String =
    uriForGrid(gridTile, kwargs)

 }