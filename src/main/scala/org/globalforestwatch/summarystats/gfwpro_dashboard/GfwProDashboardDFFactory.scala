package org.globalforestwatch.summarystats.gfwpro_dashboard

import io.circe.syntax._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{CombinedFeatureId, FeatureId, GadmFeatureId, GfwProFeatureId}


case class GfwProDashboardDFFactory(
                                     featureType: String,
                                     dataRDD: RDD[(FeatureId, GfwProDashboardData)],
                                     spark: SparkSession,
                                     kwargs: Map[String, Any]
                                   ) {

  import spark.implicits._

  def getDataFrame: DataFrame = {
    featureType match {
      case "gfwpro" => getFeatureDataFrame
      case _ =>
        throw new IllegalArgumentException("Not a valid GfwProFeatureId")
    }
  }

  private def getFeatureDataFrame: DataFrame = {
    dataRDD
      .map {
        case (id, data) =>
          id match {
            case combinedId: CombinedFeatureId =>

              val gfwproFeatureId = combinedId.featureId1 match {
                case f: GfwProFeatureId => f
                case _ => throw new RuntimeException("Not a GfwProFeatureId")
              }
              val gadmId = combinedId.featureId2 match {
                case f: GadmFeatureId => f
                case _ => throw new RuntimeException("Not a GadmFeatureId")
              }

              GfwProDashboardRowSimple(
                gfwproFeatureId.listId.asJson.noSpaces,
                gfwproFeatureId.locationId.asJson.noSpaces,
                gadmId.toString,
                data.gladAlertsCoverage.toString,
                data.gladAlertsDaily.toJson,
                data.gladAlertsWeekly.toJson,
                data.gladAlertsMonthly.toJson,
                data.viirsAlertsDaily.toJson
              )
            case _ =>
              throw new IllegalArgumentException("Not a CombinedFeatureId[GfwProFeatureId, GadmFeatureId]")
          }
      }
      .toDF(
        "list_id",
        "location_id",
        "gadm_id",
        "glad_alerts_coverage", // gladAlertsCoverage
        "glad_alerts_daily", // gladAlertsDaily
        "glad_alerts_weekly", // gladAlertsWeekly
        "glad_alerts_monthly", // gladAlertsMonthly
        "viirs_alerts_daily", // viirsAlertsDaily

      )
  }
}
