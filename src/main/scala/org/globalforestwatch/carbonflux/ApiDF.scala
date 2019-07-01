package org.globalforestwatch.carbonflux

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ApiDF {


  private def setZeroNull(column: Column): Column = when(column =!= 0.0, column)

  def unpackValues(df: DataFrame): DataFrame = {

    implicit val spark: SparkSession = df.sparkSession
    import spark.implicits._

    validatePresenceOfColumns(
      df,
      Seq(
        "feature_id",
        "threshold",
        "layers",
        "extent_2000",
        "total_area",
        "total_biomass",
        "avg_biomass_per_ha",
        "gross_annual_removals_carbon",
        "avg_gross_annual_removals_carbon_ha",
        "gross_cumul_removals_carbon",
        "avg_gross_cumul_removals_carbon_ha",
        "net_flux_co2",
        "avg_net_flux_co2_ha",
        "agc_emissions_year",
        "avg_agc_emissions_year",
        "bgc_emissions_year",
        "avg_bgc_emissions_year",
        "deadwood_carbon_emissions_year",
        "avg_deadwood_carbon_emissions_year",
        "litter_carbon_emissions_year",
        "avg_litter_carbon_emissions_year",
        "soil_carbon_emissions_year",
        "avg_soil_carbon_emissions_year",
        "total_carbon_emissions_year",
        "avg_carbon_emissions_year",
        "agc_2000",
        "avg_agc_2000",
        "bgc_2000",
        "avg_bgc_2000",
        "deadwood_carbon_2000",
        "avg_deadwood_carbon_2000",
        "litter_carbon_2000",
        "avg_litter_carbon_2000",
        "soil_2000_year",
        "avg_soil_carbon_2000",
        "total_carbon_2000",
        "avg_carbon_2000",
        "gross_emissions_co2",
        "avg_gross_emissions_co2",
        "year_data"
      )
    )

    df.select(
      $"feature_id.iso" as "iso",
      $"feature_id.adm1" as "adm1",
      $"feature_id.adm2" as "adm2",
      $"threshold",
      $"layers.gain" as "gain",
      $"layers.mangroveBiomassExtent" as "mangroves",
      $"layers.drivers" as "tcs",
      $"layers.ecozones" as "ecozone",
      $"layers.landRights" as "land_right",
      $"layers.wdpa" as "wdpa",
      $"layers.intactForestLandscapes" as "ifl",
      $"layers.plantations" as "plantations",
      $"layers.primaryForest" as "primary_forest",
      $"extent_2000",
      $"total_area",
      $"total_biomass",
      $"avg_biomass_per_ha",
      $"gross_annual_removals_carbon",
      $"avg_gross_annual_removals_carbon_ha",
      $"gross_cumul_removals_carbon",
      $"avg_gross_cumul_removals_carbon_ha",
      $"net_flux_co2",
      $"avg_net_flux_co2_ha",
      $"agc_emissions_year",
      $"avg_agc_emissions_year",
      $"bgc_emissions_year",
      $"avg_bgc_emissions_year",
      $"deadwood_carbon_emissions_year",
      $"avg_deadwood_carbon_emissions_year",
      $"litter_carbon_emissions_year",
      $"avg_litter_carbon_emissions_year",
      $"soil_carbon_emissions_year",
      $"avg_soil_carbon_emissions_year",
      $"total_carbon_emissions_year",
      $"avg_carbon_emissions_year",
      $"agc_2000",
      $"avg_agc_2000",
      $"bgc_2000",
      $"avg_bgc_2000",
      $"deadwood_carbon_2000",
      $"avg_deadwood_carbon_2000",
      $"litter_carbon_2000",
      $"avg_litter_carbon_2000",
      $"soil_2000_year",
      $"avg_soil_carbon_2000",
      $"total_carbon_2000",
      $"avg_carbon_2000",
      $"gross_emissions_co2",
      $"avg_gross_emissions_co2",
      //      'year_data.getItem(0).getItem("year") as "year_2001",
      //      'year_data.getItem(1).getItem("year") as "year_2002",
      //      'year_data.getItem(2).getItem("year") as "year_2003",
      //      'year_data.getItem(3).getItem("year") as "year_2004",
      //      'year_data.getItem(4).getItem("year") as "year_2005",
      //      'year_data.getItem(5).getItem("year") as "year_2006",
      //      'year_data.getItem(6).getItem("year") as "year_2007",
      //      'year_data.getItem(7).getItem("year") as "year_2008",
      //      'year_data.getItem(8).getItem("year") as "year_2009",
      //      'year_data.getItem(9).getItem("year") as "year_2010",
      //      'year_data.getItem(10).getItem("year") as "year_2011",
      //      'year_data.getItem(11).getItem("year") as "year_2012",
      //      'year_data.getItem(12).getItem("year") as "year_2013",
      //      'year_data.getItem(13).getItem("year") as "year_2014",
      //      'year_data.getItem(14).getItem("year") as "year_2015",
      //      'year_data.getItem(15).getItem("year") as "year_2016",
      //      'year_data.getItem(16).getItem("year") as "year_2017",
      //      'year_data.getItem(17).getItem("year") as "year_2018",
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
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2001",
      'year_data
        .getItem(1)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2002",
      'year_data
        .getItem(2)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2003",
      'year_data
        .getItem(3)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2004",
      'year_data
        .getItem(4)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2005",
      'year_data
        .getItem(5)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2006",
      'year_data
        .getItem(6)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2007",
      'year_data
        .getItem(7)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2008",
      'year_data
        .getItem(8)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2009",
      'year_data
        .getItem(9)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2010",
      'year_data
        .getItem(10)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2011",
      'year_data
        .getItem(11)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2012",
      'year_data
        .getItem(12)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2013",
      'year_data
        .getItem(13)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2014",
      'year_data
        .getItem(14)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2015",
      'year_data
        .getItem(15)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2016",
      'year_data
        .getItem(16)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2017",
      'year_data
        .getItem(17)
        .getItem("gross_emissions_co2") as "gross_emissions_co2_2018"
    )
  }

  def setNull(df: DataFrame): DataFrame = {

    val zeroColumns = df
      .select(
        "extent_2000",
        "total_area",
        "total_biomass",
        "avg_biomass_per_ha",
        "gross_annual_removals_carbon",
        "avg_gross_annual_removals_carbon_ha",
        "gross_cumul_removals_carbon",
        "avg_gross_cumul_removals_carbon_ha",
        "net_flux_co2",
        "avg_net_flux_co2_ha",
        "agc_emissions_year",
        "avg_agc_emissions_year",
        "bgc_emissions_year",
        "avg_bgc_emissions_year",
        "deadwood_carbon_emissions_year",
        "avg_deadwood_carbon_emissions_year",
        "litter_carbon_emissions_year",
        "avg_litter_carbon_emissions_year",
        "soil_carbon_emissions_year",
        "avg_soil_carbon_emissions_year",
        "total_carbon_emissions_year",
        "avg_carbon_emissions_year",
        "agc_2000",
        "avg_agc_2000",
        "bgc_2000",
        "avg_bgc_2000",
        "deadwood_carbon_2000",
        "avg_deadwood_carbon_2000",
        "litter_carbon_2000",
        "avg_litter_carbon_2000",
        "soil_2000_year",
        "avg_soil_carbon_2000",
        "total_carbon_2000",
        "avg_carbon_2000",
        "gross_emissions_co2",
        "avg_gross_emissions_co2",
        "area_loss_2001",
        "area_loss_2002",
        "area_loss_2003",
        "area_loss_2004",
        "area_loss_2005",
        "area_loss_2006",
        "area_loss_2007",
        "area_loss_2008",
        "area_loss_2009",
        "area_loss_2010",
        "area_loss_2011",
        "area_loss_2012",
        "area_loss_2013",
        "area_loss_2014",
        "area_loss_2015",
        "area_loss_2016",
        "area_loss_2017",
        "area_loss_2018",
        "biomass_loss_2001",
        "biomass_loss_2002",
        "biomass_loss_2003",
        "biomass_loss_2004",
        "biomass_loss_2005",
        "biomass_loss_2006",
        "biomass_loss_2007",
        "biomass_loss_2008",
        "biomass_loss_2009",
        "biomass_loss_2010",
        "biomass_loss_2011",
        "biomass_loss_2012",
        "biomass_loss_2013",
        "biomass_loss_2014",
        "biomass_loss_2015",
        "biomass_loss_2016",
        "biomass_loss_2017",
        "biomass_loss_2018",
        "gross_emissions_co2_2001",
        "gross_emissions_co2_2002",
        "gross_emissions_co2_2003",
        "gross_emissions_co2_2004",
        "gross_emissions_co2_2005",
        "gross_emissions_co2_2006",
        "gross_emissions_co2_2007",
        "gross_emissions_co2_2008",
        "gross_emissions_co2_2009",
        "gross_emissions_co2_2010",
        "gross_emissions_co2_2011",
        "gross_emissions_co2_2012",
        "gross_emissions_co2_2013",
        "gross_emissions_co2_2014",
        "gross_emissions_co2_2015",
        "gross_emissions_co2_2016",
        "gross_emissions_co2_2017",
        "gross_emissions_co2_2018"
      )
      .columns

    var tempDF = df

    zeroColumns.map(column => {
      tempDF = tempDF.withColumn(column, setZeroNull(col(column)))
    })

    tempDF
  }

}
