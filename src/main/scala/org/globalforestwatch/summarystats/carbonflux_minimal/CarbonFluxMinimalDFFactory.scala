package org.globalforestwatch.summarystats.carbonflux_minimal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, GadmFeatureId, SimpleFeatureId, WdpaFeatureId}

case class CarbonFluxMinimalDFFactory(
                                       featureType: String,
                                       summaryRDD: RDD[(FeatureId, CarbonFluxMinimalSummary)],
                                       spark: SparkSession
) {

  import spark.implicits._

  def getDataFrame: DataFrame = {
    featureType match {
      case "gadm"    => getGadmDataFrame
      case "feature" => getFeatureDataFrame
      case "wdpa"    => getWdpaDataFrame
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
                  CarbonFluxMinimalRowGadm(gadmId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a GadmFeatureId")
              }
            }
          }
      }
      .toDF("id", "data_group", "data")
  }
  private def getFeatureDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, gladAlertSummary) =>
          gladAlertSummary.stats.map {
            case (dataGroup, data) => {
              id match {
                case simpleId: SimpleFeatureId =>
                  CarbonFluxMinimalRowSimple(simpleId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a WdpaFeatureId")
              }
            }
          }
      }
      .toDF("id", "data_group", "data")
  }
  private def getWdpaDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, gladAlertSummary) =>
          gladAlertSummary.stats.map {
            case (dataGroup, data) => {
              id match {
                case wdpaId: WdpaFeatureId =>
                  CarbonFluxMinimalRowWdpa(wdpaId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a SimpleFeatureId")
              }
            }
          }
      }
      .toDF("id", "data_group", "data")
  }
}
