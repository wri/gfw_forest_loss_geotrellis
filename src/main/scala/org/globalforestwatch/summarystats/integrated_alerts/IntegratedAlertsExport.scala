package org.globalforestwatch.summarystats.integrated_alerts

import org.apache.spark.sql.{Column, DataFrame}
import org.globalforestwatch.summarystats.SummaryExport

object IntegratedAlertsExport extends SummaryExport {
  override protected def exportGadm(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {
    summaryDF.cache()

    val spark = summaryDF.sparkSession
    import spark.implicits._

    val cols =
      List($"id.iso" as "iso", $"id.adm1" as "adm1", $"id.adm2" as "adm2")

    val gadmDF =
      summaryDF.transform(IntegratedAlertsDF.unpackValues(cols))
    summaryDF.unpersist()

    gadmDF.cache()
    exportChange(gadmDF, outputUrl)
    gadmDF.unpersist()
  }

  private def exportSummary(df: DataFrame, outputUrl: String): Unit = {

    val adm2DF = df
      .transform(IntegratedAlertsDF.aggSummary(List("iso", "adm1", "adm2")))

    adm2DF
      .coalesce(50)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/summary")

    val adm1DF = adm2DF
      .transform(IntegratedAlertsDF.aggSummary(List("iso", "adm1")))

    adm1DF
      .coalesce(30)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/summary")

    val isoDF = adm1DF
      .transform(IntegratedAlertsDF.aggSummary(List("iso")))

    isoDF
      .coalesce(20)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/summary")

  }

  private def exportWhitelist(df: DataFrame, outputUrl: String): Unit = {
    val adm2DF = df
      .transform(IntegratedAlertsDF.whitelist(List("iso", "adm1", "adm2")))

    adm2DF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/whitelist")

    val adm1DF = adm2DF
      .transform(IntegratedAlertsDF.whitelist2(List("iso", "adm1")))

    adm1DF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/whitelist")

    val isoDF = adm1DF
      .transform(IntegratedAlertsDF.whitelist2(List("iso")))

    isoDF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/whitelist")

  }

  private def exportChange(df: DataFrame, outputUrl: String): Unit = {

    val adm2DF = df
      .transform(IntegratedAlertsDF.aggChangeDaily(List("iso", "adm1", "adm2")))

    adm2DF
      .coalesce(100)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/daily_alerts")

    val adm1DF = df
      .transform(IntegratedAlertsDF.aggChangeDaily(List("iso", "adm1")))

    adm1DF
      .coalesce(50)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/daily_alerts")

    val isoDF = df
      .transform(IntegratedAlertsDF.aggChangeDaily(List("iso")))

    isoDF
      .coalesce(25)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/daily_alerts")
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

    _export(summaryDF, outputUrl + "/wdpa", kwargs, groupByCols, unpackCols, wdpa = true, numExportParts = 50)
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

    _export(summaryDF, outputUrl + "/geostore", kwargs, groupByCols, unpackCols, numExportParts = 350)
  }

  private def _export(summaryDF: DataFrame,
                      outputUrl: String,
                      kwargs: Map[String, Any],
                      groupByCols: List[String],
                      unpackCols: List[Column],
                      wdpa: Boolean = false,
                      numExportParts: Int = 10): Unit = {

    val cols = groupByCols

    val df = summaryDF.transform(
      IntegratedAlertsDF.unpackValues(unpackCols, wdpa = wdpa)
    )

    df.cache()

    df.transform(IntegratedAlertsDF.aggChangeDaily(cols, wdpa = wdpa))
      .coalesce(numExportParts)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/daily_alerts")

    df.unpersist()
    ()
  }
}
