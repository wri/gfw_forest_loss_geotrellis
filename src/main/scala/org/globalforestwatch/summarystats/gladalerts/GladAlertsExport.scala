package org.globalforestwatch.summarystats.gladalerts

import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport

object GladAlertsExport extends SummaryExport {

  override protected def exportGadm(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {

    summaryDF.cache()

    val tileDF = summaryDF
      .transform(TileDF.unpackValues)
      .transform(TileDF.sumAlerts)

    tileDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/tiles")

    val adm2DailyDF = summaryDF
      .transform(Adm2DailyDF.unpackValues)
      .transform(Adm2DailyDF.sumAlerts)

    summaryDF.unpersist()

    adm2DailyDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2_daily")

    val adm2WeeklyDF = adm2DailyDF.transform(Adm2WeeklyDF.sumAlerts)

    adm2WeeklyDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2_weekly")

    val adm1WeeklyDF = adm2WeeklyDF
      .transform(Adm1WeeklyDF.sumAlerts)

    adm1WeeklyDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1_weekly")

    val isoWeeklyDF = adm1WeeklyDF
      .transform(IsoWeeklyDF.sumAlerts)

    isoWeeklyDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso_weekly")
  }

  override protected def exportWdpa(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {
    val wdpaDailyDF = summaryDF
      .transform(WdpaDailyDF.unpackValues)
      .transform(WdpaDailyDF.sumAlerts)

    wdpaDailyDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/wdpa_daily")

    val wdpaWeeklyDF = wdpaDailyDF.transform(WdpaWeeklyDF.sumAlerts)

    wdpaWeeklyDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/wdpa_weekly")
  }
}
