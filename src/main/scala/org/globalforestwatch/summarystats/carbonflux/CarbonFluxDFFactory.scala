package org.globalforestwatch.summarystats.carbonflux

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, GadmFeatureId}

case class CarbonFluxDFFactory(featureType: String,
                               summaryRDD: RDD[(FeatureId, CarbonFluxSummary)],
                               spark: SparkSession) {

  import spark.implicits._

  def getDataFrame: DataFrame = {
    featureType match {
      case "gadm" => getGadmDataFrame
      case _ =>
        throw new IllegalArgumentException("Not a valid FeatureId")
    }
  }

  private def getGadmDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case gadmId: GadmFeatureId =>
                  CarbonFluxRow(
                    gadmId,
                    dataGroup,
                    data.extent2000,
                    data.totalArea,
                    data.totalBiomass,
                    data.avgBiomass,
                    data.totalGrossAnnualRemovalsCarbon,
                    data.avgGrossAnnualRemovalsCarbon,
                    data.totalGrossCumulRemovalsCarbon,
                    data.avgGrossCumulRemovalsCarbon,
                    data.totalNetFluxCo2,
                    data.avgNetFluxCo2,
                    data.totalAgcEmisYear,
                    data.avgAgcEmisYear,
                    data.totalBgcEmisYear,
                    data.avgBgcEmisYear,
                    data.totalDeadwoodCarbonEmisYear,
                    data.avgDeadwoodCarbonEmisYear,
                    data.totalLitterCarbonEmisYear,
                    data.avgLitterCarbonEmisYear,
                    data.totalSoilCarbonEmisYear,
                    data.avgSoilCarbonEmisYear,
                    data.totalCarbonEmisYear,
                    data.avgTotalCarbonEmisYear,
                    data.totalAgc2000,
                    data.avgAgc2000,
                    data.totalBgc2000,
                    data.avgBgc2000,
                    data.totalDeadwoodCarbon2000,
                    data.avgDeadwoodCarbon2000,
                    data.totalLitterCarbon2000,
                    data.avgLitterCarbon2000,
                    data.totalSoil2000Year,
                    data.avgSoilCarbon2000,
                    data.totalCarbon2000,
                    data.avgTotalCarbon2000,
                    data.totalGrossEmissionsCo2,
                    data.avgGrossEmissionsCo2,
                    CarbonFluxYearDataMap.toList(data.lossYear)
                  )
                case _ =>
                  throw new IllegalArgumentException("Not a GadmFeatureId")
              }
            }
          }
      }
      .toDF(
        "id",
        "data_group",
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
  }
}
