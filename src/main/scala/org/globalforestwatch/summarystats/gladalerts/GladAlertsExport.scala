package org.globalforestwatch.summarystats.gladalerts

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.DataFrame

object GladAlertsExport {

  val csvOptions: Map[String, String] = Map(
    "header" -> "true",
    "delimiter" -> ",",
    "quote" -> "\u0000",
    "quoteMode" -> "NONE",
    "nullValue" -> "\u0000"
  )

  def export(featureType: String,
             summaryDF: DataFrame,
             outputUrl: String): Unit = {

    val runOutputUrl: String = outputUrl +
      "/gladAlerts_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    featureType match {
      case "gadm" => exportGadm(summaryDF, runOutputUrl)
      case "feature" => exportFeature(summaryDF, runOutputUrl)
      case "wdpa" => exportWdpa(summaryDF, runOutputUrl)
      case _ =>
        throw new IllegalArgumentException(
          "Feature type must be one of 'gadm' and 'feature'"
        )
    }
  }

  private def exportGadm(summaryDF: DataFrame, runOutputUrl: String): Unit = {

    summaryDF.cache()

    val tileDF = summaryDF
      .transform(TileDF.sumAlerts)

    tileDF.write
      .options(csvOptions)
      .csv(path = runOutputUrl + "/tiles")

    val adm2DailyDF = summaryDF
      .transform(Adm2DailyDF.unpackValues)
      .transform(Adm2DailyDF.sumAlerts)

    summaryDF.unpersist()

    adm2DailyDF.write
      .options(csvOptions)
      .csv(path = runOutputUrl + "/adm2_daily")

    val adm2WeeklyDF = adm2DailyDF.transform(Adm2WeeklyDF.sumAlerts)

    adm2WeeklyDF.write
      .options(csvOptions)
      .csv(path = runOutputUrl + "/adm2_weekly")

    val adm1WeeklyDF = adm2WeeklyDF
      .transform(Adm1WeeklyDF.sumAlerts)

    adm1WeeklyDF.write
      .options(csvOptions)
      .csv(path = runOutputUrl + "/adm1_weekly")

    val isoWeeklyDF = adm1WeeklyDF
      .transform(IsoWeeklyDF.sumAlerts)

    isoWeeklyDF.write
      .options(csvOptions)
      .csv(path = runOutputUrl + "/iso_weekly")
  }

  private def exportFeature(summaryDF: DataFrame,
                            runOutputUrl: String): Unit = {}

  private def exportWdpa(summaryDF: DataFrame, runOutputUrl: String): Unit = {
    val wdpaDailyDF = summaryDF
      .transform(WdpaDailyDF.unpackValues)
      .transform(WdpaDailyDF.sumAlerts)

    wdpaDailyDF.write
      .options(csvOptions)
      .csv(path = runOutputUrl + "/wdpa_daily")

    val wdpaWeeklyDF = wdpaDailyDF.transform(WdpaWeeklyDF.sumAlerts)

    wdpaWeeklyDF.write
      .options(csvOptions)
      .csv(path = runOutputUrl + "/wdpa_weekly")
  }
}
