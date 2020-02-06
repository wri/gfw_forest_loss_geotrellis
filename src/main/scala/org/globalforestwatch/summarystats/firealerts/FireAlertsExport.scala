package org.globalforestwatch.summarystats.firealerts

import org.apache.spark.sql.{Column, DataFrame}
import org.globalforestwatch.summarystats.SummaryExport


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

    val adm2DF = adm2DailyDF
      .transform(FireAlertsDF.aggChangeWeekly(List("iso", "adm1", "adm2")))

    adm2DF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/weekly_alerts")

    val adm1DF = adm2DF
      .transform(FireAlertsDF.aggChangeWeekly2(List("iso", "adm1")))

    adm1DF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/weekly_alerts")


    val isoDF = adm1DF
      .transform(FireAlertsDF.aggChangeWeekly2(List("iso")))


    isoDF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/weekly_alerts")

    gadmDF.unpersist()
  }

  override protected def exportWdpa(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {

    val spark = summaryDF.sparkSession
    import spark.implicits._

    val groupByCols = List(
      "wdpa_protected_area__id",
      "wdpa_protected_area__name",
      "wdpa_protected_area__iucn_cat",
      "wdpa_protected_area__iso",
      "wdpa_protected_area__status"
    )
    val unpackCols = List(
      $"id.wdpaId" as "wdpa_protected_area__id",
      $"id.name" as "wdpa_protected_area__name",
      $"id.iucnCat" as "wdpa_protected_area__iucn_cat",
      $"id.iso" as "wdpa_protected_area__iso",
      $"id.status" as "wdpa_protected_area__status"
    )

    _export(summaryDF, outputUrl + "/wdpa", kwargs, groupByCols, unpackCols, wdpa = true)
  }

  override protected def exportFeature(summaryDF: DataFrame,
                                       outputUrl: String,
                                       kwargs: Map[String, Any]): Unit = {

    val spark = summaryDF.sparkSession
    import spark.implicits._

    val groupByCols = List("feature__id")
    val unpackCols = List($"id.featureId" as "feature__id")

    _export(summaryDF, outputUrl + "/feature", kwargs, groupByCols, unpackCols)
  }

  override protected def exportGeostore(summaryDF: DataFrame,
                                        outputUrl: String,
                                        kwargs: Map[String, Any]): Unit = {

    val spark = summaryDF.sparkSession
    import spark.implicits._


    val groupByCols = List("geostore__id")
    val unpackCols = List($"id.geostoreId" as "geostore__id")

    _export(summaryDF, outputUrl + "/geostore", kwargs, groupByCols, unpackCols)

  }

  private def _export(summaryDF: DataFrame,
                      outputUrl: String,
                      kwargs: Map[String, Any],
                      groupByCols: List[String],
                      unpackCols: List[Column],
                      wdpa: Boolean = false): Unit = {

    val cols = groupByCols

    val df = summaryDF.transform(
      FireAlertsDF.unpackValues(unpackCols)
    )

    df.cache()

    df.transform(FireAlertsDF.aggChangeDaily(cols))
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/daily_alerts")

    df.transform(FireAlertsDF.aggChangeWeekly(cols))
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/weekly_alerts")

    df.unpersist()
    ()
  }
}
