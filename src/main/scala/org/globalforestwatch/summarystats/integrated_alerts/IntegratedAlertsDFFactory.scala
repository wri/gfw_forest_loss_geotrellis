package org.globalforestwatch.summarystats.integrated_alerts

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{
  FeatureId,
  GadmFeatureId,
  GeostoreFeatureId,
  SimpleFeatureId,
  WdpaFeatureId
}

case class IntegratedAlertsDFFactory(
  featureType: String,
  summaryRDD: RDD[(FeatureId, IntegratedAlertsSummary)],
  spark: SparkSession
) {

  import spark.implicits._

  def getDataFrame: DataFrame = {
    featureType match {
      case "gadm" => getGadmDataFrame
      case "feature" => getFeatureDataFrame
      case "wdpa" => getWdpaDataFrame
      case "geostore" => getGeostoreDataFrame
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
                  IntegratedAlertsRowGadm(gadmId, dataGroup, data)
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
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case simpleId: SimpleFeatureId =>
                  IntegratedAlertsRowSimple(simpleId, dataGroup, data)
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
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case wdpaId: WdpaFeatureId =>
                  IntegratedAlertsRowWdpa(wdpaId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a SimpleFeatureId")
              }
            }
          }
      }
      .toDF("id", "data_group", "data")
  }

  private def getGeostoreDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case geostoreId: GeostoreFeatureId =>
                  IntegratedAlertsRowGeostore(geostoreId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a GeostoreFeatureId")
              }
            }
          }
      }
      .toDF("id", "data_group", "data")
  }
}
