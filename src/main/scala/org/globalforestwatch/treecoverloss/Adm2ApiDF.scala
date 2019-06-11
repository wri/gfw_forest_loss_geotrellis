package org.globalforestwatch.treecoverloss
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Adm2ApiDF {

  val spark: SparkSession = TreeLossSparkSession()

  import spark.implicits._

  def nestYearData(df: DataFrame): DataFrame = {
    
    df.select(
      $"iso",
      $"adm1",
      $"adm2",
      $"threshold",
      $"tcs",
      $"global_land_cover",
      $"primary_forest",
      $"idn_primary_forest",
      $"erosion",
      $"biodiversity_significance",
      $"biodiversity_intactness",
      $"wdpa",
      $"aze",
      $"plantations",
      $"river_basin",
      $"ecozone",
      $"urban_watershed",
      $"mangroves_1996",
      $"mangroves_2016",
      $"water_stress",
      $"ifl",
      $"endemic_bird_area",
      $"tiger_cl",
      $"landmark",
      $"land_right",
      $"kba",
      $"mining",
      $"rspo",
      $"idn_mys_peatlands",
      $"oil_palm",
      $"idn_forest_moratorium",
      $"idn_land_cover",
      $"mex_protected_areas",
      $"mex_pes",
      $"mex_forest_zoning",
      $"per_production_forest",
      $"per_protected_area",
      $"per_forest_concession",
      $"bra_biomes",
      $"wood_fiber",
      $"resource_right",
      $"managed_forests",
      $"oil_gas",
      $"total_area",
      $"extent_2000",
      $"extent_2010",
      $"total_gain",
      $"total_biomass",
      $"avg_biomass_per_ha" * $"extent_2000" as "weighted_biomass_per_ha",
      $"total_co2",
      $"total_mangrove_biomass",
      $"avg_mangrove_biomass_per_ha" * $"extent_2000" as "weighted_mangrove_biomass_per_ha",
      $"total_mangrove_co2",
      array(
        struct(
          $"year_2001" as "year",
          $"area_loss_2001" as "area_loss",
          $"biomass_loss_2001" as "biomass_loss",
          $"carbon_emissions_2001" as "carbon_emissions",
          $"mangrove_biomass_loss_2001" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2001" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2002" as "year",
          $"area_loss_2002" as "area_loss",
          $"biomass_loss_2002" as "biomass_loss",
          $"carbon_emissions_2002" as "carbon_emissions",
          $"mangrove_biomass_loss_2002" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2002" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2003" as "year",
          $"area_loss_2003" as "area_loss",
          $"biomass_loss_2003" as "biomass_loss",
          $"carbon_emissions_2003" as "carbon_emissions",
          $"mangrove_biomass_loss_2003" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2003" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2004" as "year",
          $"area_loss_2004" as "area_loss",
          $"biomass_loss_2004" as "biomass_loss",
          $"carbon_emissions_2004" as "carbon_emissions",
          $"mangrove_biomass_loss_2004" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2004" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2005" as "year",
          $"area_loss_2005" as "area_loss",
          $"biomass_loss_2005" as "biomass_loss",
          $"carbon_emissions_2005" as "carbon_emissions",
          $"mangrove_biomass_loss_2005" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2005" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2006" as "year",
          $"area_loss_2006" as "area_loss",
          $"biomass_loss_2006" as "biomass_loss",
          $"carbon_emissions_2006" as "carbon_emissions",
          $"mangrove_biomass_loss_2006" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2006" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2007" as "year",
          $"area_loss_2007" as "area_loss",
          $"biomass_loss_2007" as "biomass_loss",
          $"carbon_emissions_2007" as "carbon_emissions",
          $"mangrove_biomass_loss_2007" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2007" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2008" as "year",
          $"area_loss_2008" as "area_loss",
          $"biomass_loss_2008" as "biomass_loss",
          $"carbon_emissions_2008" as "carbon_emissions",
          $"mangrove_biomass_loss_2008" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2008" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2009" as "year",
          $"area_loss_2009" as "area_loss",
          $"biomass_loss_2009" as "biomass_loss",
          $"carbon_emissions_2009" as "carbon_emissions",
          $"mangrove_biomass_loss_2009" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2009" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2010" as "year",
          $"area_loss_2010" as "area_loss",
          $"biomass_loss_2010" as "biomass_loss",
          $"carbon_emissions_2010" as "carbon_emissions",
          $"mangrove_biomass_loss_2010" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2010" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2011" as "year",
          $"area_loss_2011" as "area_loss",
          $"biomass_loss_2011" as "biomass_loss",
          $"carbon_emissions_2011" as "carbon_emissions",
          $"mangrove_biomass_loss_2011" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2011" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2012" as "year",
          $"area_loss_2012" as "area_loss",
          $"biomass_loss_2012" as "biomass_loss",
          $"carbon_emissions_2012" as "carbon_emissions",
          $"mangrove_biomass_loss_2012" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2012" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2013" as "year",
          $"area_loss_2013" as "area_loss",
          $"biomass_loss_2013" as "biomass_loss",
          $"carbon_emissions_2013" as "carbon_emissions",
          $"mangrove_biomass_loss_2013" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2013" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2014" as "year",
          $"area_loss_2014" as "area_loss",
          $"biomass_loss_2014" as "biomass_loss",
          $"carbon_emissions_2014" as "carbon_emissions",
          $"mangrove_biomass_loss_2014" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2014" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2015" as "year",
          $"area_loss_2015" as "area_loss",
          $"biomass_loss_2015" as "biomass_loss",
          $"carbon_emissions_2015" as "carbon_emissions",
          $"mangrove_biomass_loss_2015" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2015" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2016" as "year",
          $"area_loss_2016" as "area_loss",
          $"biomass_loss_2016" as "biomass_loss",
          $"carbon_emissions_2016" as "carbon_emissions",
          $"mangrove_biomass_loss_2016" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2016" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2017" as "year",
          $"area_loss_2017" as "area_loss",
          $"biomass_loss_2017" as "biomass_loss",
          $"carbon_emissions_2017" as "carbon_emissions",
          $"mangrove_biomass_loss_2017" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2017" as "mangrove_carbon_emissions"
        ),
        struct(
          $"year_2018" as "year",
          $"area_loss_2018" as "area_loss",
          $"biomass_loss_2018" as "biomass_loss",
          $"carbon_emissions_2018" as "carbon_emissions",
          $"mangrove_biomass_loss_2018" as "mangrove_biomass_loss",
          $"mangrove_carbon_emissions_2018" as "mangrove_carbon_emissions"
        )
      ) as "year_data"
    )

  }
}
