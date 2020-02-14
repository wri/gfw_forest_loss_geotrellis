package org.globalforestwatch.summarystats.firealerts

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FireAlertFeatureId, GadmFeatureId, SimpleFeatureId, WdpaFeatureId, GeostoreFeatureId}

case class FireAlertsDFFactory(
                                featureType: String,
                                summaryRDD: RDD[(FireAlertFeatureId, FireAlertsSummary)],
                                spark: SparkSession
) {

  import spark.implicits._

  def getDataFrame: DataFrame = {
    featureType match {
      case "gadm"    => getGadmDataFrame
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
                case fireId: FireAlertFeatureId =>
                  fireId.feature match {
                    case gadmId: GadmFeatureId =>
                      FireAlertsRowGadm(gadmId, fireId.alertDate, dataGroup, data)
                    case _ =>
                      throw new IllegalArgumentException("Not a supported feature ID type for fire alerts")
                  }
              }
            }
          }
      }
      .toDF("id", "alertDate", "data_group", "data")
  }

  private def getWdpaDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case fireId: FireAlertFeatureId =>
                  fireId.feature match {
                    case wdpaId: WdpaFeatureId =>
                      FireAlertsRowWdpa(wdpaId, fireId.alertDate, dataGroup, data)
                    case _ =>
                      throw new IllegalArgumentException("Not a supported feature ID type for fire alerts")
                  }
              }
            }
          }
      }
      .toDF("id", "alertDate", "data_group", "data")
  }

  private def getFeatureDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case fireId: FireAlertFeatureId =>
                  fireId.feature match {
                    case featureId: SimpleFeatureId =>
                      FireAlertsRowSimple(featureId, fireId.alertDate, dataGroup, data)
                    case _ =>
                      throw new IllegalArgumentException("Not a supported feature ID type for fire alerts")
                  }
              }
            }
          }
      }
      .toDF("id", "alertDate", "data_group", "data")
  }

  private def getGeostoreDataFrame: DataFrame = {
    summaryRDD
      .flatMap {
        case (id, summary) =>
          summary.stats.map {
            case (dataGroup, data) => {
              id match {
                case fireId: FireAlertFeatureId =>
                  fireId.feature match {
                    case geostoreId: GeostoreFeatureId =>
                      FireAlertsRowGeostore(geostoreId, fireId.alertDate, dataGroup, data)
                    case _ =>
                      throw new IllegalArgumentException("Not a supported feature ID type for fire alerts")
                  }
              }
            }
          }
      }
      .toDF("id", "alertDate", "data_group", "data")
  }
}
