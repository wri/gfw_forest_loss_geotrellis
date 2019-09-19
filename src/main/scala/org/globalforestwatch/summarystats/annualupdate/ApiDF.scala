package org.globalforestwatch.summarystats.annualupdate

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql._

object ApiDF {

  def unpackValues(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(df, Seq("id", "data_group", "data"))

    df.select(
      $"id.iso" as "iso",
      $"id.adm1" as "adm1",
      $"id.adm2" as "adm2",
      $"data_group.lossYear" as "loss_year",
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
      $"data.extent2000" as "treecover_extent_2000_ha",
      $"data.extent2010" as "treecover_extent_2010_ha",
      $"data.totalArea" as "total_area_ha",
      $"data.totalGainArea" as "total_gain_2000-2012_ha",
      $"data.totalBiomass" as "total_biomass_stock_2000_Mt",
      $"data.totalCo2" as "total_co2_stock_Mt",
      $"data.totalMangroveBiomass" as "total_mangrove_biomass_stock_Mt",
      $"data.totalMangroveCo2" as "total_mangrove_co2_stock_Mt",
      $"data.areaLoss" as "area_loss_ha",
      $"data.biomassLoss" as "biomass_loss_Mt",
      $"data.co2Emissions" as "co2_emissions_Mt",
      $"data.mangroveBiomassLoss" as "mangrove_biomass_loss_Mt",
      $"data.mangroveCo2Emissions" as "mangrove_co2_emissions_Mt"
    )
  }

  //  private def setZeroNull(column: Column): Column = when(column =!= 0.0, column)
  //
  //  //  private def setFalseNull(column: Column): Column =
  //  //    when(column === true, column)
  //
  //  def setNull(df: DataFrame): DataFrame = {
  //
  //    val zeroColumns = df
  //      .select(
  //        "total_area",
  //        "extent_2000",
  //        "extent_2010",
  //        "total_gain",
  //        "total_biomass",
  //        "avg_biomass_per_ha",
  //        "total_co2",
  //        "total_mangrove_biomass",
  //        "avg_mangrove_biomass_per_ha",
  //        "total_mangrove_co2",
  //        "area_loss_2001",
  //        "biomass_loss_2001",
  //        "carbon_emissions_2001",
  //        "mangrove_biomass_loss_2001",
  //        "mangrove_carbon_emissions_2001",
  //        "area_loss_2002",
  //        "biomass_loss_2002",
  //        "carbon_emissions_2002",
  //        "mangrove_biomass_loss_2002",
  //        "mangrove_carbon_emissions_2002",
  //        "area_loss_2003",
  //        "biomass_loss_2003",
  //        "carbon_emissions_2003",
  //        "mangrove_biomass_loss_2003",
  //        "mangrove_carbon_emissions_2003",
  //        "area_loss_2004",
  //        "biomass_loss_2004",
  //        "carbon_emissions_2004",
  //        "mangrove_biomass_loss_2004",
  //        "mangrove_carbon_emissions_2004",
  //        "area_loss_2005",
  //        "biomass_loss_2005",
  //        "carbon_emissions_2005",
  //        "mangrove_biomass_loss_2005",
  //        "mangrove_carbon_emissions_2005",
  //        "area_loss_2006",
  //        "biomass_loss_2006",
  //        "carbon_emissions_2006",
  //        "mangrove_biomass_loss_2006",
  //        "mangrove_carbon_emissions_2006",
  //        "area_loss_2007",
  //        "biomass_loss_2007",
  //        "carbon_emissions_2007",
  //        "mangrove_biomass_loss_2007",
  //        "mangrove_carbon_emissions_2007",
  //        "area_loss_2008",
  //        "biomass_loss_2008",
  //        "carbon_emissions_2008",
  //        "mangrove_biomass_loss_2008",
  //        "mangrove_carbon_emissions_2008",
  //        "area_loss_2009",
  //        "biomass_loss_2009",
  //        "carbon_emissions_2009",
  //        "mangrove_biomass_loss_2009",
  //        "mangrove_carbon_emissions_2009",
  //        "area_loss_2010",
  //        "biomass_loss_2010",
  //        "carbon_emissions_2010",
  //        "mangrove_biomass_loss_2010",
  //        "mangrove_carbon_emissions_2010",
  //        "area_loss_2011",
  //        "biomass_loss_2011",
  //        "carbon_emissions_2011",
  //        "mangrove_biomass_loss_2011",
  //        "mangrove_carbon_emissions_2011",
  //        "area_loss_2012",
  //        "biomass_loss_2012",
  //        "carbon_emissions_2012",
  //        "mangrove_biomass_loss_2012",
  //        "mangrove_carbon_emissions_2012",
  //        "area_loss_2013",
  //        "biomass_loss_2013",
  //        "carbon_emissions_2013",
  //        "mangrove_biomass_loss_2013",
  //        "mangrove_carbon_emissions_2013",
  //        "area_loss_2014",
  //        "biomass_loss_2014",
  //        "carbon_emissions_2014",
  //        "mangrove_biomass_loss_2014",
  //        "mangrove_carbon_emissions_2014",
  //        "area_loss_2015",
  //        "biomass_loss_2015",
  //        "carbon_emissions_2015",
  //        "mangrove_biomass_loss_2015",
  //        "mangrove_carbon_emissions_2015",
  //        "area_loss_2016",
  //        "biomass_loss_2016",
  //        "carbon_emissions_2016",
  //        "mangrove_biomass_loss_2016",
  //        "mangrove_carbon_emissions_2016",
  //        "area_loss_2017",
  //        "biomass_loss_2017",
  //        "carbon_emissions_2017",
  //        "mangrove_biomass_loss_2017",
  //        "mangrove_carbon_emissions_2017",
  //        "area_loss_2018",
  //        "biomass_loss_2018",
  //        "carbon_emissions_2018",
  //        "mangrove_biomass_loss_2018",
  //        "mangrove_carbon_emissions_2018"
  //      )
  //      .columns
  //
  //    //    val falseColumns = df
  //    //      .select(
  //    //        "primary_forest",
  //    //        "idn_primary_forest",
  //    //        "biodiversity_significance",
  //    //        "biodiversity_intactness",
  //    //        "aze",
  //    //        "urban_watershed",
  //    //        "mangroves_1996",
  //    //        "mangroves_2016",
  //    //        "endemic_bird_area",
  //    //        "tiger_cl",
  //    //        "landmark",
  //    //        "land_right",
  //    //        "kba",
  //    //        "mining",
  //    //        "idn_mys_peatlands",
  //    //        "oil_palm",
  //    //        "idn_forest_moratorium",
  //    //        "mex_protected_areas",
  //    //        "mex_pes",
  //    //        "per_production_forest",
  //    //        "per_protected_area",
  //    //        "wood_fiber",
  //    //        "resource_right",
  //    //        "managed_forests",
  //    //        "oil_gas"
  //    //      )
  //    //      .columns
  //
  //    var tempDF = df
  //
  //    zeroColumns.map(column => {
  //      tempDF = tempDF.withColumn(column, setZeroNull(col(column)))
  //    })
  //
  //    //    falseColumns.map(column => {
  //    //      tempDF = tempDF.withColumn(column, setFalseNull(col(column)))
  //    //    })
  //
  //    tempDF
  //  }

}
