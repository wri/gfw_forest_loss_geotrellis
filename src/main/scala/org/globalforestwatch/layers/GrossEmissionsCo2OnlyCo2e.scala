package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossEmissionsCo2OnlyCo2e(gridTile: GridTile,
                                     model: String = "standard", kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "gfw_full_extent_co2_gross_emissions"

  val uri: String =
    s"s3://gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/standard/20220316/${gridTile.tileId}_gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_biomass_soil_2001_21.tif"
}
