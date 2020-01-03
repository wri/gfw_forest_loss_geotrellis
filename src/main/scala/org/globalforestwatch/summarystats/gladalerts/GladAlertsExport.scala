package org.globalforestwatch.summarystats.gladalerts

import org.apache.spark.sql.{Column, DataFrame}
import org.globalforestwatch.summarystats.SummaryExport
import org.globalforestwatch.util.Util.getAnyMapValue

object GladAlertsExport extends SummaryExport {

  val minZoom = 0
  val maxZoom = 12

  override protected def exportGadm(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {

    val changeOnly: Boolean =
      getAnyMapValue[Boolean](kwargs, "changeOnly")

    val buildDataCube: Boolean =
      getAnyMapValue[Boolean](kwargs, "buildDataCube")

    val minZoom: Int = if (buildDataCube) minZoom else maxZoom


    summaryDF.cache()
    exportTiles(summaryDF, outputUrl, buildDataCube)

    val spark = summaryDF.sparkSession
    import spark.implicits._

    val cols =
      List($"id.iso" as "iso", $"id.adm1" as "adm1", $"id.adm2" as "adm2")

    val gadmDF =
      summaryDF.transform(GladAlertsDF.unpackValues(cols, minZoom = minZoom))
    summaryDF.unpersist()

    gadmDF.cache()
    if (!changeOnly) {
      exportWhitelist(gadmDF, outputUrl)
      exportSummary(gadmDF, outputUrl)
    }
    exportChange(gadmDF, outputUrl)
    gadmDF.unpersist()
  }

  private def exportTiles(tileDF: DataFrame,
                          outputUrl: String,
                          buildDataCube: Boolean): Unit = {

    if (buildDataCube) {
      tileDF
        .transform(GladAlertsTileDF.unpackValues)
        .transform(GladAlertsTileDF.sumAlerts)
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/tiles")
    }
  }

  private def exportSummary(df: DataFrame, outputUrl: String): Unit = {

    val adm2DF = df
      .transform(GladAlertsDF.aggSummary(List("iso", "adm1", "adm2")))

    adm2DF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/summary")

    val adm1DF = adm2DF
      .transform(GladAlertsDF.aggSummary(List("iso", "adm1")))

    adm1DF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/summary")

    val isoDF = adm1DF
      .transform(GladAlertsDF.aggSummary(List("iso")))

    isoDF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/summary")

  }

  private def exportWhitelist(df: DataFrame, outputUrl: String): Unit = {

    val adm2DF = df
      .transform(GladAlertsDF.whitelist(List("iso", "adm1", "adm2")))

    adm2DF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/whitelist")

    val adm1DF = adm2DF
      .transform(GladAlertsDF.whitelist2(List("iso", "adm1")))

    adm1DF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/whitelist")

    val isoDF = adm1DF
      .transform(GladAlertsDF.whitelist2(List("iso")))

    isoDF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/whitelist")

  }

  private def exportChange(df: DataFrame, outputUrl: String): Unit = {

    val adm2DailyDF = df
      .transform(GladAlertsDF.aggChangeDaily(List("iso", "adm1", "adm2")))

    adm2DailyDF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/daily_alerts")

    val adm2DF = adm2DailyDF
      .transform(GladAlertsDF.aggChangeWeekly(List("iso", "adm1", "adm2")))

    adm2DF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/weekly_alerts")

    val adm1DF = adm2DF
      .transform(GladAlertsDF.aggChangeWeekly(List("iso", "adm1")))

    adm1DF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/weekly_alerts")

    val isoDF = adm1DF
      .transform(GladAlertsDF.aggChangeWeekly(List("iso")))

    isoDF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/weekly_alerts")
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
      $"id.wdpa_id" as "wdpa_protected_area__id",
      $"id.name" as "wdpa_protected_area__name",
      $"id.iucn_cat" as "wdpa_protected_area__iucn_cat",
      $"id.iso" as "wdpa_protected_area__iso",
      $"id.status" as "wdpa_protected_area__status"
    )

    _export(summaryDF, outputUrl + "/wdpa", kwargs, groupByCols, unpackCols)
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
                      unpackCols: List[Column]): Unit = {

    val changeOnly: Boolean = getAnyMapValue[Boolean](kwargs, "changeOnly")

    val buildDataCube: Boolean =
      getAnyMapValue[Boolean](kwargs, "buildDataCube")

    val minZoom: Int = if (buildDataCube) minZoom else maxZoom

    val cols = groupByCols

    val df = summaryDF.transform(
      GladAlertsDF.unpackValues(unpackCols, minZoom = minZoom)
    )

    df.cache()

    if (!changeOnly) {

      df.transform(GladAlertsDF.whitelist(cols))
        .coalesce(1)
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/whitelist")

      df.transform(GladAlertsDF.aggSummary(cols))
        .coalesce(1)
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/summary")
    }

    df.transform(GladAlertsDF.aggChangeDaily(cols))
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/daily_alerts")

    df.transform(GladAlertsDF.aggChangeWeekly(cols))
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/weekly_alerts")

    df.unpersist()
    ()
  }
}
