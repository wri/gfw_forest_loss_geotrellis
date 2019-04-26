package org.globalforestwatch.treecoverloss

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ApiDF {

  //    val spark: SparkSession = TreeLossSparkSession.spark
//
//  import spark.implicits._

  private def setZeroNull(column: Column): Column = when(column =!= 0.0, column)

  private def setFalseNull(column: Column): Column =
    when(column === true, column)

  def setNull(df: DataFrame): DataFrame = {

    val zeroColumns = df
      .select(
        "totalarea",
        "extent2000",
        "extent2010",
        "total_gain",
        "total_biomass",
        "avg_biomass_per_ha",
        "total_co2",
        "total_mangrove_biomass",
        "avg_mangrove_biomass_per_ha",
        "total_mangrove_co2",
        "area_loss_2001",
        "biomass_loss_2001",
        "carbon_emissions_2001",
        "mangrove_biomass_loss_2001",
        "mangrove_carbon_emissions_2001",
        "area_loss_2002",
        "biomass_loss_2002",
        "carbon_emissions_2002",
        "mangrove_biomass_loss_2002",
        "mangrove_carbon_emissions_2002",
        "area_loss_2003",
        "biomass_loss_2003",
        "carbon_emissions_2003",
        "mangrove_biomass_loss_2003",
        "mangrove_carbon_emissions_2003",
        "area_loss_2004",
        "biomass_loss_2004",
        "carbon_emissions_2004",
        "mangrove_biomass_loss_2004",
        "mangrove_carbon_emissions_2004",
        "area_loss_2005",
        "biomass_loss_2005",
        "carbon_emissions_2005",
        "mangrove_biomass_loss_2005",
        "mangrove_carbon_emissions_2005",
        "area_loss_2006",
        "biomass_loss_2006",
        "carbon_emissions_2006",
        "mangrove_biomass_loss_2006",
        "mangrove_carbon_emissions_2006",
        "area_loss_2007",
        "biomass_loss_2007",
        "carbon_emissions_2007",
        "mangrove_biomass_loss_2007",
        "mangrove_carbon_emissions_2007",
        "area_loss_2008",
        "biomass_loss_2008",
        "carbon_emissions_2008",
        "mangrove_biomass_loss_2008",
        "mangrove_carbon_emissions_2008",
        "area_loss_2009",
        "biomass_loss_2009",
        "carbon_emissions_2009",
        "mangrove_biomass_loss_2009",
        "mangrove_carbon_emissions_2009",
        "area_loss_2010",
        "biomass_loss_2010",
        "carbon_emissions_2010",
        "mangrove_biomass_loss_2010",
        "mangrove_carbon_emissions_2010",
        "area_loss_2011",
        "biomass_loss_2011",
        "carbon_emissions_2011",
        "mangrove_biomass_loss_2011",
        "mangrove_carbon_emissions_2011",
        "area_loss_2012",
        "biomass_loss_2012",
        "carbon_emissions_2012",
        "mangrove_biomass_loss_2012",
        "mangrove_carbon_emissions_2012",
        "area_loss_2013",
        "biomass_loss_2013",
        "carbon_emissions_2013",
        "mangrove_biomass_loss_2013",
        "mangrove_carbon_emissions_2013",
        "area_loss_2014",
        "biomass_loss_2014",
        "carbon_emissions_2014",
        "mangrove_biomass_loss_2014",
        "mangrove_carbon_emissions_2014",
        "area_loss_2015",
        "biomass_loss_2015",
        "carbon_emissions_2015",
        "mangrove_biomass_loss_2015",
        "mangrove_carbon_emissions_2015",
        "area_loss_2016",
        "biomass_loss_2016",
        "carbon_emissions_2016",
        "mangrove_biomass_loss_2016",
        "mangrove_carbon_emissions_2016",
        "area_loss_2017",
        "biomass_loss_2017",
        "carbon_emissions_2017",
        "mangrove_biomass_loss_2017",
        "mangrove_carbon_emissions_2017",
        "area_loss_2018",
        "biomass_loss_2018",
        "carbon_emissions_2018",
        "mangrove_biomass_loss_2018",
        "mangrove_carbon_emissions_2018"
      )
      .columns
    val falseColumns = df
      .select(
        "primary_forest",
        "idn_primary_forest",
        "biodiversity_significance",
        "biodiversity_intactness",
        "aze",
        "urban_watershed",
        "mangroves_1996",
        "mangroves_2016",
        "endemic_bird_area",
        "tiger_cl",
        "landmark",
        "land_right",
        "kba",
        "mining",
        "idn_mys_peatlands",
        "oil_palm",
        "idn_forest_moratorium",
        "mex_protected_areas",
        "mex_pes",
        "per_production_forest",
        "per_protected_area",
        "wood_fiber",
        "resource_right",
        "managed_forests",
        "oil_gas"
      )
      .columns

    var tempDF = df

    zeroColumns.map(column => {
      tempDF = tempDF.withColumn(column, setZeroNull(col(column)))
    })

    falseColumns.map(column => {
      tempDF = tempDF.withColumn(column, setFalseNull(col(column)))
    })

    tempDF
  }

}
