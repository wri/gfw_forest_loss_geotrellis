package org.globalforestwatch.treecoverloss

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Adm1SummaryDF {

    def sumArea(spark: SparkSession)(df: DataFrame): DataFrame = {
        import spark.implicits._
    df.groupBy($"iso", $"adm1", $"threshold")
      .agg(
        round(sum("area_ha")) as "area_ha",
        round(sum("extent_2000_ha")) as "extent_2000_ha",
        round(sum("extent_2010_ha")) as "extent_2010_ha",
        round(sum("gain_2000_2012_ha")) as "gain_2000_2012_ha",
        round(sum("biomass_Mt")) as "biomass_Mt",
        round(
            sum($"avg_biomass_per_ha_Mt" * $"extent_2000_ha") / sum(
                "extent_2000_ha"
          )
        ) as "avg_biomass_per_ha_Mt",
        round(sum("carbon_Mt")) as "carbon_Mt",
        round(sum("tc_loss_ha_2001")) as "tc_loss_ha_2001",
        round(sum("tc_loss_ha_2002")) as "tc_loss_ha_2002",
        round(sum("tc_loss_ha_2003")) as "tc_loss_ha_2003",
        round(sum("tc_loss_ha_2004")) as "tc_loss_ha_2004",
        round(sum("tc_loss_ha_2005")) as "tc_loss_ha_2005",
        round(sum("tc_loss_ha_2006")) as "tc_loss_ha_2006",
        round(sum("tc_loss_ha_2007")) as "tc_loss_ha_2007",
        round(sum("tc_loss_ha_2008")) as "tc_loss_ha_2008",
        round(sum("tc_loss_ha_2009")) as "tc_loss_ha_2009",
        round(sum("tc_loss_ha_2010")) as "tc_loss_ha_2010",
        round(sum("tc_loss_ha_2011")) as "tc_loss_ha_2011",
        round(sum("tc_loss_ha_2012")) as "tc_loss_ha_2012",
        round(sum("tc_loss_ha_2013")) as "tc_loss_ha_2013",
        round(sum("tc_loss_ha_2014")) as "tc_loss_ha_2014",
        round(sum("tc_loss_ha_2015")) as "tc_loss_ha_2015",
        round(sum("tc_loss_ha_2016")) as "tc_loss_ha_2016",
        round(sum("tc_loss_ha_2017")) as "tc_loss_ha_2017",
        round(sum("tc_loss_ha_2018")) as "tc_loss_ha_2018",
        round(sum("biomass_loss_Mt_2001")) as "biomass_loss_Mt_2001",
        round(sum("biomass_loss_Mt_2002")) as "biomass_loss_Mt_2002",
        round(sum("biomass_loss_Mt_2003")) as "biomass_loss_Mt_2003",
        round(sum("biomass_loss_Mt_2004")) as "biomass_loss_Mt_2004",
        round(sum("biomass_loss_Mt_2005")) as "biomass_loss_Mt_2005",
        round(sum("biomass_loss_Mt_2006")) as "biomass_loss_Mt_2006",
        round(sum("biomass_loss_Mt_2007")) as "biomass_loss_Mt_2007",
        round(sum("biomass_loss_Mt_2008")) as "biomass_loss_Mt_2008",
        round(sum("biomass_loss_Mt_2009")) as "biomass_loss_Mt_2009",
        round(sum("biomass_loss_Mt_2010")) as "biomass_loss_Mt_2010",
        round(sum("biomass_loss_Mt_2011")) as "biomass_loss_Mt_2011",
        round(sum("biomass_loss_Mt_2012")) as "biomass_loss_Mt_2012",
        round(sum("biomass_loss_Mt_2013")) as "biomass_loss_Mt_2013",
        round(sum("biomass_loss_Mt_2014")) as "biomass_loss_Mt_2014",
        round(sum("biomass_loss_Mt_2015")) as "biomass_loss_Mt_2015",
        round(sum("biomass_loss_Mt_2016")) as "biomass_loss_Mt_2016",
        round(sum("biomass_loss_Mt_2017")) as "biomass_loss_Mt_2017",
        round(sum("biomass_loss_Mt_2018")) as "biomass_loss_Mt_2018",
        round(sum("carbon_emissions_Mt_2001")) as "carbon_emissions_Mt_2001",
        round(sum("carbon_emissions_Mt_2002")) as "carbon_emissions_Mt_2002",
        round(sum("carbon_emissions_Mt_2003")) as "carbon_emissions_Mt_2003",
        round(sum("carbon_emissions_Mt_2004")) as "carbon_emissions_Mt_2004",
        round(sum("carbon_emissions_Mt_2005")) as "carbon_emissions_Mt_2005",
        round(sum("carbon_emissions_Mt_2006")) as "carbon_emissions_Mt_2006",
        round(sum("carbon_emissions_Mt_2007")) as "carbon_emissions_Mt_2007",
        round(sum("carbon_emissions_Mt_2008")) as "carbon_emissions_Mt_2008",
        round(sum("carbon_emissions_Mt_2009")) as "carbon_emissions_Mt_2009",
        round(sum("carbon_emissions_Mt_2010")) as "carbon_emissions_Mt_2010",
        round(sum("carbon_emissions_Mt_2011")) as "carbon_emissions_Mt_2011",
        round(sum("carbon_emissions_Mt_2012")) as "carbon_emissions_Mt_2012",
        round(sum("carbon_emissions_Mt_2013")) as "carbon_emissions_Mt_2013",
        round(sum("carbon_emissions_Mt_2014")) as "carbon_emissions_Mt_2014",
        round(sum("carbon_emissions_Mt_2015")) as "carbon_emissions_Mt_2015",
        round(sum("carbon_emissions_Mt_2016")) as "carbon_emissions_Mt_2016",
        round(sum("carbon_emissions_Mt_2017")) as "carbon_emissions_Mt_2017",
        round(sum("carbon_emissions_Mt_2018")) as "carbon_emissions_Mt_2018"
      )
  }
}
