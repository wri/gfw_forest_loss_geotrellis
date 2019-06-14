package org.globalforestwatch.annualupdate_minimal

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql._

object Adm2DF {

  val spark: SparkSession = TreeLossSparkSession()

  import spark.implicits._

  def unpackValues(df: DataFrame): DataFrame = {

    validatePresenceOfColumns(
      df,
      Seq(
        "feature_id",
        "threshold",
        "layers",
        "extent_2000",
        "extent_2010",
        "total_area",
        "total_gain",
        "total_biomass",
        "avg_biomass_per_ha",
        "total_co2",
//        "total_mangrove_biomass",
//        "avg_mangrove_biomass_per_ha",
//        "total_mangrove_co2",
        "year_data"
      )
    )

    df.select(
      $"feature_id.iso" as "iso",
      $"feature_id.adm1" as "adm1",
      $"feature_id.adm2" as "adm2",
      $"threshold",
      $"layers.drivers" as "tcs",
      $"layers.globalLandCover" as "global_land_cover",
      $"layers.primaryForest" as "primary_forest",
//      $"layers.idnPrimaryForest" as "idn_primary_forest",
//      $"layers.erosion" as "erosion",
//      $"layers.biodiversitySignificance" as "biodiversity_significance",
//      $"layers.biodiversityIntactness" as "biodiversity_intactness",
      $"layers.wdpa" as "wdpa",
      $"layers.aze" as "aze",
      $"layers.plantations" as "plantations",
//      $"layers.riverBasins" as "river_basin",
//      $"layers.ecozones" as "ecozone",
//      $"layers.urbanWatersheds" as "urban_watershed",
//      $"layers.mangroves1996" as "mangroves_1996",
//      $"layers.mangroves2016" as "mangroves_2016",
//      $"layers.waterStress" as "water_stress",
      $"layers.intactForestLandscapes" as "ifl",
//      $"layers.endemicBirdAreas" as "endemic_bird_area",
      $"layers.tigerLandscapes" as "tiger_cl",
      $"layers.landmark" as "landmark",
      $"layers.landRights" as "land_right",
      $"layers.keyBiodiversityAreas" as "kba",
      $"layers.mining" as "mining",
//      $"layers.rspo" as "rspo",
//      $"layers.peatlands" as "idn_mys_peatlands",
      $"layers.oilPalm" as "oil_palm",
      $"layers.idnForestMoratorium" as "idn_forest_moratorium",
//      $"layers.idnLandCover" as "idn_land_cover",
//      $"layers.mexProtectedAreas" as "mex_protected_areas",
//      $"layers.mexPaymentForEcosystemServices" as "mex_pes",
//      $"layers.mexForestZoning" as "mex_forest_zoning",
//      $"layers.perProductionForest" as "per_production_forest",
//      $"layers.perProtectedAreas" as "per_protected_area",
//      $"layers.perForestConcessions" as "per_forest_concession",
//      $"layers.braBiomes" as "bra_biomes",
      $"layers.woodFiber" as "wood_fiber",
      $"layers.resourceRights" as "resource_right",
      $"layers.logging" as "managed_forests",
//      $"layers.oilGas" as "oil_gas",
      $"extent_2000",
      $"extent_2010",
      $"total_area",
      $"total_gain",
      $"total_biomass",
      $"avg_biomass_per_ha",
      $"total_co2",
//      $"total_mangrove_biomass",
//      $"avg_mangrove_biomass_per_ha",
//      $"total_mangrove_co2",
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
        .getItem("carbon_emissions") as "carbon_emissions_2018"
//      'year_data
//        .getItem(0)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2001",
//      'year_data
//        .getItem(1)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2002",
//      'year_data
//        .getItem(2)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2003",
//      'year_data
//        .getItem(3)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2004",
//      'year_data
//        .getItem(4)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2005",
//      'year_data
//        .getItem(5)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2006",
//      'year_data
//        .getItem(6)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2007",
//      'year_data
//        .getItem(7)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2008",
//      'year_data
//        .getItem(8)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2009",
//      'year_data
//        .getItem(9)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2010",
//      'year_data
//        .getItem(10)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2011",
//      'year_data
//        .getItem(11)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2012",
//      'year_data
//        .getItem(12)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2013",
//      'year_data
//        .getItem(13)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2014",
//      'year_data
//        .getItem(14)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2015",
//      'year_data
//        .getItem(15)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2016",
//      'year_data
//        .getItem(16)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2017",
//      'year_data
//        .getItem(17)
//        .getItem("mangrove_biomass_loss") as "mangrove_biomass_loss_2018",
//      'year_data
//        .getItem(0)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2001",
//      'year_data
//        .getItem(1)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2002",
//      'year_data
//        .getItem(2)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2003",
//      'year_data
//        .getItem(3)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2004",
//      'year_data
//        .getItem(4)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2005",
//      'year_data
//        .getItem(5)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2006",
//      'year_data
//        .getItem(6)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2007",
//      'year_data
//        .getItem(7)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2008",
//      'year_data
//        .getItem(8)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2009",
//      'year_data
//        .getItem(9)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2010",
//      'year_data
//        .getItem(10)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2011",
//      'year_data
//        .getItem(11)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2012",
//      'year_data
//        .getItem(12)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2013",
//      'year_data
//        .getItem(13)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2014",
//      'year_data
//        .getItem(14)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2015",
//      'year_data
//        .getItem(15)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2016",
//      'year_data
//        .getItem(16)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2017",
//      'year_data
//        .getItem(17)
//        .getItem("mangrove_carbon_emissions") as "mangrove_carbon_emissions_2018"
    )
  }

}
