package org.globalforestwatch.summarystats.gfwpro_dashboard

import io.circe.syntax._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, SimpleFeatureId}


case class GfwProDashboardDFFactory(
                                     featureType: String,
                                     summaryRDD: RDD[(FeatureId, GfwProDashboardSummary)],
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
    val analysisRDD: RDD[(SimpleFeatureId, GfwProDashboardData)] = summaryRDD
      .flatMap {
        case (id, summary) =>
          // We need to convert the Map to a List in order to correctly flatmap the data
          summary.stats.toList.map {
            case (dataGroup, data) =>
              id match {
                case featureId: SimpleFeatureId =>
                  (
                    featureId,
                    GfwProDashboardData(
                      dataGroup.alertCoverage,
                      gladAlertsDaily = GfwProDashboardDataDateCount
                        .fill(dataGroup.alertDate, data.alertCount),
                      gladAlertsWeekly = GfwProDashboardDataDateCount
                        .fill(dataGroup.alertDate, data.alertCount, weekly = true),
                      gladAlertsMonthly = GfwProDashboardDataDateCount.fill(
                        dataGroup.alertDate,
                        data.alertCount,
                        monthly = true
                      ),
                      viirsAlertsDaily = GfwProDashboardDataDateCount.empty
                    )
                  )
                case _ =>
                  throw new IllegalArgumentException("Not a SimpleFeatureId")
              }

          }
      }
      .reduceByKey(_ merge _)

    val fireCount: RDD[(SimpleFeatureId, GfwProDashboardDataDateCount)] =
      GfwProDashboardAnalysis.fireStats(featureType, spark, kwargs)

    analysisRDD
      .leftOuterJoin(fireCount)
      .mapValues {
        case (data, fire) =>
          data.update(
            viirsAlertsDaily =
              fire.getOrElse(GfwProDashboardDataDateCount.empty)
          )
      }
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
