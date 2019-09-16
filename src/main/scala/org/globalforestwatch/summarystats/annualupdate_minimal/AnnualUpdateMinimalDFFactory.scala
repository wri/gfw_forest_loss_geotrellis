package org.globalforestwatch.summarystats.annualupdate_minimal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, GadmFeatureId}

case class AnnualUpdateMinimalDFFactory(
  featureType: String,
  summaryRDD: RDD[(FeatureId, AnnualUpdateMinimalSummary)],
  spark: SparkSession
) {
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
                  AnnualUpdateMinimalRow(
                    gadmId,
                    dataGroup,
                    data.extent2000,
                    data.extent2010,
                    data.totalArea,
                    data.totalGainArea,
                    data.totalBiomass,
                    data.totalCo2,
                    data.biomassHistogram.mean(),
                    AnnualUpdateMinimalYearDataMap.toList(data.lossYear)
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
        "extent_2010",
        "total_area",
        "total_gain",
        "total_biomass",
        "total_co2",
        "avg_biomass_per_ha",
        "year_data"
      )
  }
}
