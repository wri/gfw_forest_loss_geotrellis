package org.globalforestwatch.treecoverloss

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Adm2SummaryDF {

    val spark: SparkSession = TreeLossSparkSession()

  import spark.implicits._

  def sumArea(df: DataFrame): DataFrame = {

    df.groupBy($"iso", $"adm1", $"adm2", $"threshold")
      .agg(
        round(sum("totalarea")) as "area_ha",
        round(sum("extent2000")) as "extent_2000_ha",
        round(sum("extent2010")) as "extent_2010_ha",
        round(sum("total_gain")) as "gain_2000_2012_ha",
        round(sum("total_biomass")) as "biomass_Mt",
        round(sum($"avg_biomass_per_ha" * $"extent2000") / sum("extent2000")) as "avg_biomass_per_ha_Mt",
        round(sum("total_co2")) as "carbon_Mt",
        round(sum("area_loss_2001")) as "tc_loss_ha_2001",
        round(sum("area_loss_2002")) as "tc_loss_ha_2002",
        round(sum("area_loss_2003")) as "tc_loss_ha_2003",
        round(sum("area_loss_2004")) as "tc_loss_ha_2004",
        round(sum("area_loss_2005")) as "tc_loss_ha_2005",
        round(sum("area_loss_2006")) as "tc_loss_ha_2006",
        round(sum("area_loss_2007")) as "tc_loss_ha_2007",
        round(sum("area_loss_2008")) as "tc_loss_ha_2008",
        round(sum("area_loss_2009")) as "tc_loss_ha_2009",
        round(sum("area_loss_2010")) as "tc_loss_ha_2010",
        round(sum("area_loss_2011")) as "tc_loss_ha_2011",
        round(sum("area_loss_2012")) as "tc_loss_ha_2012",
        round(sum("area_loss_2013")) as "tc_loss_ha_2013",
        round(sum("area_loss_2014")) as "tc_loss_ha_2014",
        round(sum("area_loss_2015")) as "tc_loss_ha_2015",
        round(sum("area_loss_2016")) as "tc_loss_ha_2016",
        round(sum("area_loss_2017")) as "tc_loss_ha_2017",
        round(sum("area_loss_2018")) as "tc_loss_ha_2018",
        round(sum("biomass_loss_2001")) as "biomass_loss_Mt_2001",
        round(sum("biomass_loss_2002")) as "biomass_loss_Mt_2002",
        round(sum("biomass_loss_2003")) as "biomass_loss_Mt_2003",
        round(sum("biomass_loss_2004")) as "biomass_loss_Mt_2004",
        round(sum("biomass_loss_2005")) as "biomass_loss_Mt_2005",
        round(sum("biomass_loss_2006")) as "biomass_loss_Mt_2006",
        round(sum("biomass_loss_2007")) as "biomass_loss_Mt_2007",
        round(sum("biomass_loss_2008")) as "biomass_loss_Mt_2008",
        round(sum("biomass_loss_2009")) as "biomass_loss_Mt_2009",
        round(sum("biomass_loss_2010")) as "biomass_loss_Mt_2010",
        round(sum("biomass_loss_2011")) as "biomass_loss_Mt_2011",
        round(sum("biomass_loss_2012")) as "biomass_loss_Mt_2012",
        round(sum("biomass_loss_2013")) as "biomass_loss_Mt_2013",
        round(sum("biomass_loss_2014")) as "biomass_loss_Mt_2014",
        round(sum("biomass_loss_2015")) as "biomass_loss_Mt_2015",
        round(sum("biomass_loss_2016")) as "biomass_loss_Mt_2016",
        round(sum("biomass_loss_2017")) as "biomass_loss_Mt_2017",
        round(sum("biomass_loss_2018")) as "biomass_loss_Mt_2018",
        round(sum("carbon_emissions_2001")) as "carbon_emissions_Mt_2001",
        round(sum("carbon_emissions_2002")) as "carbon_emissions_Mt_2002",
        round(sum("carbon_emissions_2003")) as "carbon_emissions_Mt_2003",
        round(sum("carbon_emissions_2004")) as "carbon_emissions_Mt_2004",
        round(sum("carbon_emissions_2005")) as "carbon_emissions_Mt_2005",
        round(sum("carbon_emissions_2006")) as "carbon_emissions_Mt_2006",
        round(sum("carbon_emissions_2007")) as "carbon_emissions_Mt_2007",
        round(sum("carbon_emissions_2008")) as "carbon_emissions_Mt_2008",
        round(sum("carbon_emissions_2009")) as "carbon_emissions_Mt_2009",
        round(sum("carbon_emissions_2010")) as "carbon_emissions_Mt_2010",
        round(sum("carbon_emissions_2011")) as "carbon_emissions_Mt_2011",
        round(sum("carbon_emissions_2012")) as "carbon_emissions_Mt_2012",
        round(sum("carbon_emissions_2013")) as "carbon_emissions_Mt_2013",
        round(sum("carbon_emissions_2014")) as "carbon_emissions_Mt_2014",
        round(sum("carbon_emissions_2015")) as "carbon_emissions_Mt_2015",
        round(sum("carbon_emissions_2016")) as "carbon_emissions_Mt_2016",
        round(sum("carbon_emissions_2017")) as "carbon_emissions_Mt_2017",
        round(sum("carbon_emissions_2018")) as "carbon_emissions_Mt_2018"
      )
  }
}
