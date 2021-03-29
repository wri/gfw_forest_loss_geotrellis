package org.globalforestwatch.summarystats.gfwpro_dashboard

import io.circe.syntax._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, SimpleFeatureId}


case class GfwProDashboardDFFactory(
                                     featureType: String,
                                     dataRDD: RDD[(FeatureId, GfwProDashboardData)],
                                     spark: SparkSession,
                                     kwargs: Map[String, Any]
                                   ) {

  import spark.implicits._

  def getDataFrame: DataFrame = {
    featureType match {
      case "feature" => getFeatureDataFrame
      case _ =>
        throw new IllegalArgumentException("Not a valid FeatureId")
    }
  }

  private def getFeatureDataFrame: DataFrame = {
    dataRDD
      .map {
        case (id, data) =>
          id match {
            case simpleId: SimpleFeatureId =>
              GfwProDashboardRowSimple(
                simpleId.featureId.asJson.noSpaces,
                data.gladAlertsCoverage.toString,
                data.gladAlertsDaily.toJson,
                data.gladAlertsWeekly.toJson,
                data.gladAlertsMonthly.toJson,
                data.viirsAlertsDaily.toJson
              )
            case _ =>
              throw new IllegalArgumentException("Not a SimpleFeatureId")
          }
      }
      .toDF(
        "location_id",
        "glad_alerts_coverage", // gladAlertsCoverage
        "glad_alerts_daily", // gladAlertsDaily
        "glad_alerts_weekly", // gladAlertsWeekly
        "glad_alerts_monthly", // gladAlertsMonthly
        "viirs_alerts_daily", // viirsAlertsDaily

      )
  }
}
