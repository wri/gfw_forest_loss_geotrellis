package org.globalforestwatch.summarystats.firealerts

import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport
import org.globalforestwatch.summarystats.treecoverloss.TreeLossDF
import org.globalforestwatch.summarystats.treecoverloss.TreeLossExport.csvOptions
import org.globalforestwatch.util.Util.getAnyMapValue

object FireAlertsExport extends SummaryExport {

  override protected def exportGadm(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {
    val spark = summaryDF.sparkSession
    import spark.implicits._

    val cols =
      List($"id.iso" as "iso", $"id.adm1" as "adm1", $"id.adm2" as "adm2")

    val gadmDF =
      summaryDF.transform(FireAlertsDF.unpackValues(cols))

    gadmDF.cache()
    val adm2DailyDF = gadmDF
      .transform(FireAlertsDF.aggChangeDaily(List("iso", "adm1", "adm2")))

    adm2DailyDF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/daily_alerts")

    gadmDF.unpersist()
  }

  override protected def exportFeature(summaryDF: DataFrame,
                                       outputUrl: String,
                                       kwargs: Map[String, Any]): Unit = {

    val spark = summaryDF.sparkSession
    import spark.implicits._

    val unpackCols = List($"id.geostoreId" as "geostore__id")
    val groupByCols = List("geostore__id")

    val df = summaryDF.transform(
      FireAlertsDF.unpackValues(unpackCols)
    )

    df
      .transform(FireAlertsDF.aggChangeDaily(groupByCols))
      .coalesce(1)
      .orderBy($"geostore__id")
      .write
      .options(csvOptions)
      .csv(path = outputUrl)
  }
}
