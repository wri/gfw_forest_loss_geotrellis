package org.globalforestwatch.summarystats.firealerts

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, FireAlertFeatureId, GadmFeatureId, SimpleFeatureId, WdpaFeatureId}

case class FireAlertsDFFactory(
                                featureType: String,
                                summaryRDD: RDD[(FireAlertFeatureId, FireAlertsSummary)],
                                spark: SparkSession
) {

  import spark.implicits._

  def getDataFrame: DataFrame = {
    featureType match {
      case "gadm"    => getGadmDataFrame
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
}
