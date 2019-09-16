package org.globalforestwatch.summarystats.annualupdate

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql._

object Adm2DF {

  def unpackValues(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "id",
        "data_group",
        "extent_2000",
        "extent_2010",
        "total_area",
        "total_gain",
        "total_biomass",
        "avg_biomass_per_ha",
        "total_co2",
        "total_mangrove_biomass",
        "avg_mangrove_biomass_per_ha",
        "total_mangrove_co2",
        "year_data"
      )
    )

    df.select(
      $"id.iso" as "iso",
      $"id.adm1" as "adm1",
      $"id.adm2" as "adm2",
      $"data_group.threshold" as "threshold",
      $"data_group.drivers" as "tcs",
      $"data_group.globalLandCover" as "global_land_cover",
      $"data_group.primaryForest" as "primary_forest",
      $"data_group.idnPrimaryForest" as "idn_primary_forest",
      $"data_group.erosion" as "erosion",
      $"data_group.biodiversitySignificance" as "biodiversity_significance",
      $"data_group.biodiversityIntactness" as "biodiversity_intactness",
      $"data_group.wdpa" as "wdpa",
      $"data_group.aze" as "aze",
      $"data_group.plantations" as "plantations",
      $"data_group.riverBasins" as "river_basin",
      $"data_group.ecozones" as "ecozone",
      $"data_group.urbanWatersheds" as "urban_watershed",
      $"data_group.mangroves1996" as "mangroves_1996",
      $"data_group.mangroves2016" as "mangroves_2016",
      $"data_group.waterStress" as "water_stress",
      $"data_group.intactForestLandscapes" as "ifl",
      $"data_group.endemicBirdAreas" as "endemic_bird_area",
      $"data_group.tigerLandscapes" as "tiger_cl",
      $"data_group.landmark" as "landmark",
      $"data_group.landRights" as "land_right",
      $"data_group.keyBiodiversityAreas" as "kba",
      $"data_group.mining" as "mining",
      $"data_group.rspo" as "rspo",
      $"data_group.peatlands" as "idn_mys_peatlands",
      $"data_group.oilPalm" as "oil_palm",
      $"data_group.idnForestMoratorium" as "idn_forest_moratorium",
      $"data_group.idnLandCover" as "idn_land_cover",
      $"data_group.mexProtectedAreas" as "mex_protected_areas",
      $"data_group.mexPaymentForEcosystemServices" as "mex_pes",
      $"data_group.mexForestZoning" as "mex_forest_zoning",
      $"data_group.perProductionForest" as "per_production_forest",
      $"data_group.perProtectedAreas" as "per_protected_area",
      $"data_group.perForestConcessions" as "per_forest_concession",
      $"data_group.braBiomes" as "bra_biomes",
      $"data_group.woodFiber" as "wood_fiber",
      $"data_group.resourceRights" as "resource_right",
      $"data_group.logging" as "managed_forests",
      $"data_group.oilGas" as "oil_gas",
      $"extent_2000",
      $"extent_2010",
      $"total_area",
      $"total_gain",
      $"total_biomass",
      $"avg_biomass_per_ha",
      $"total_co2",
      $"total_mangrove_biomass",
      $"avg_mangrove_biomass_per_ha",
      $"total_mangrove_co2",
      'year_data.getItem(0).getItem("year") as "year_2001",
      'year_data.getItem(1).getItem("year") as "year_2002",
      'year_data.getItem(2).getItem("year") as "year_2003",
      'year_data.getItem(3).getItem("year") as "year_2004",
      'year_data.getItem(4).getItem("year") as "year_2005",
      'year_data.getItem(5).getItem("year") as "year_2006",
      'year_data.getItem(6).getItem("year") as "year_2007",
      'year_data.getItem(7).getItem("year") as "year_2008",
      'year_data.getItem(8).getItem("year") as "year_2009",
      'year_data.getItem(9).getItem("year") as "year_2010",
      'year_data.getItem(10).getItem("year") as "year_2011",
      'year_data.getItem(11).getItem("year") as "year_2012",
      'year_data.getItem(12).getItem("year") as "year_2013",
      'year_data.getItem(13).getItem("year") as "year_2014",
      'year_data.getItem(14).getItem("year") as "year_2015",
      'year_data.getItem(15).getItem("year") as "year_2016",
      'year_data.getItem(16).getItem("year") as "year_2017",
      'year_data.getItem(17).getItem("year") as "year_2018",
      'year_data.getItem(0).getItem("area_loss") as "area_loss_2001",
      'year_data.getItem(1).getItem("area_loss") as "area_loss_2002",
      'year_data.getItem(2).getItem("area_loss") as "area_loss_2003",
      'year_data.getItem(3).getItem("area_loss") as "area_loss_2004",
      'year_data.getItem(4).getItem("area_loss") as "area_loss_2005",
      'year_data.getItem(5).getItem("area_loss") as "area_loss_2006",
      'year_data.getItem(6).getItem("area_loss") as "area_loss_2007",
      'year_data.getItem(7).getItem("area_loss") as "area_loss_2008",
      'year_data.getItem(8).getItem("area_loss") as "area_loss_2009",
      'year_data.getItem(9).getItem("area_loss") as "area_loss_2010",
      'year_data.getItem(10).getItem("area_loss") as "area_loss_2011",
      'year_data.getItem(11).getItem("area_loss") as "area_loss_2012",
      'year_data.getItem(12).getItem("area_loss") as "area_loss_2013",
      'year_data.getItem(13).getItem("area_loss") as "area_loss_2014",
      'year_data.getItem(14).getItem("area_loss") as "area_loss_2015",
      'year_data.getItem(15).getItem("area_loss") as "area_loss_2016",
      'year_data.getItem(16).getItem("area_loss") as "area_loss_2017",
      'year_data.getItem(17).getItem("area_loss") as "area_loss_2018",
      'year_data
        .getItem(0)
        .getItem("biomass_loss") as "biomass_loss_2001",
      'year_data
        .getItem(1)
        .getItem("biomass_loss") as "biomass_loss_2002",
      'year_data
        .getItem(2)
        .getItem("biomass_loss") as "biomass_loss_2003",
      'year_data
        .getItem(3)
        .getItem("biomass_loss") as "biomass_loss_2004",
      'year_data
        .getItem(4)
        .getItem("biomass_loss") as "biomass_loss_2005",
      'year_data
        .getItem(5)
        .getItem("biomass_loss") as "biomass_loss_2006",
      'year_data
        .getItem(6)
        .getItem("biomass_loss") as "biomass_loss_2007",
      'year_data
        .getItem(7)
        .getItem("biomass_loss") as "biomass_loss_2008",
      'year_data
        .getItem(8)
        .getItem("biomass_loss") as "biomass_loss_2009",
      'year_data
        .getItem(9)
        .getItem("biomass_loss") as "biomass_loss_2010",
      'year_data
        .getItem(10)
        .getItem("biomass_loss") as "biomass_loss_2011",
      'year_data
        .getItem(11)
        .getItem("biomass_loss") as "biomass_loss_2012",
      'year_data
        .getItem(12)
        .getItem("biomass_loss") as "biomass_loss_2013",
      'year_data
        .getItem(13)
        .getItem("biomass_loss") as "biomass_loss_2014",
      'year_data
        .getItem(14)
        .getItem("biomass_loss") as "biomass_loss_2015",
      'year_data
        .getItem(15)
        .getItem("biomass_loss") as "biomass_loss_2016",
      'year_data
        .getItem(16)
        .getItem("biomass_loss") as "biomass_loss_2017",
      'year_data
        .getItem(17)
        .getItem("biomass_loss") as "biomass_loss_2018",
      'year_data
        .getItem(0)
        .getItem("carbon_emissions") as "carbon_emissions_2001",
      'year_data
        .getItem(1)
        .getItem("carbon_emissions") as "carbon_emissions_2002",
      'year_data
        .getItem(2)
        .getItem("carbon_emissions") as "carbon_emissions_2003",
      'year_data
        .getItem(3)
        .getItem("carbon_emissions") as "carbon_emissions_2004",
      'year_data
        .getItem(4)
        .getItem("carbon_emissions") as "carbon_emissions_2005",
      'year_data
        .getItem(5)
        .getItem("carbon_emissions") as "carbon_emissions_2006",
      'year_data
        .getItem(6)
        .getItem("carbon_emissions") as "carbon_emissions_2007",
      'year_data
        .getItem(7)
        .getItem("carbon_emissions") as "carbon_emissions_2008",
      'year_data
        .getItem(8)
        .getItem("carbon_emissions") as "carbon_emissions_2009",
      'year_data
        .getItem(9)
        .getItem("carbon_emissions") as "carbon_emissions_2010",
      'year_data
        .getItem(10)
        .getItem("carbon_emissions") as "carbon_emissions_2011",
      'year_data
        .getItem(11)
        .getItem("carbon_emissions") as "carbon_emissions_2012",
      'year_data
        .getItem(12)
        .getItem("carbon_emissions") as "carbon_emissions_2013",
      'year_data
        .getItem(13)
        .getItem("carbon_emissions") as "carbon_emissions_2014",
      'year_data
        .getItem(14)
        .getItem("carbon_emissions") as "carbon_emissions_2015",
      'year_data
        .getItem(15)
        .getItem("carbon_emissions") as "carbon_emissions_2016",
      'year_data
        .getItem(16)
        .getItem("carbon_emissions") as "carbon_emissions_2017",
      'year_data
        .getItem(17)
        .getItem("carbon_emissions") as "carbon_emissions_2018",
      'year_data
        .getItem(0)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2001",
      'year_data
        .getItem(1)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2002",
      'year_data
        .getItem(2)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2003",
      'year_data
        .getItem(3)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2004",
      'year_data
        .getItem(4)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2005",
      'year_data
        .getItem(5)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2006",
      'year_data
        .getItem(6)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2007",
      'year_data
        .getItem(7)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2008",
      'year_data
        .getItem(8)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2009",
      'year_data
        .getItem(9)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2010",
      'year_data
        .getItem(10)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2011",
      'year_data
        .getItem(11)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2012",
      'year_data
        .getItem(12)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2013",
      'year_data
        .getItem(13)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2014",
      'year_data
        .getItem(14)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2015",
      'year_data
        .getItem(15)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2016",
      'year_data
        .getItem(16)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2017",
      'year_data
        .getItem(17)
        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2018",
      'year_data
        .getItem(0)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2001",
      'year_data
        .getItem(1)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2002",
      'year_data
        .getItem(2)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2003",
      'year_data
        .getItem(3)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2004",
      'year_data
        .getItem(4)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2005",
      'year_data
        .getItem(5)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2006",
      'year_data
        .getItem(6)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2007",
      'year_data
        .getItem(7)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2008",
      'year_data
        .getItem(8)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2009",
      'year_data
        .getItem(9)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2010",
      'year_data
        .getItem(10)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2011",
      'year_data
        .getItem(11)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2012",
      'year_data
        .getItem(12)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2013",
      'year_data
        .getItem(13)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2014",
      'year_data
        .getItem(14)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2015",
      'year_data
        .getItem(15)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2016",
      'year_data
        .getItem(16)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2017",
      'year_data
        .getItem(17)
        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2018"
    )
  }

}
