package org.globalforestwatch.summarystats.firealerts

import math.ceil
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.globalforestwatch.summarystats.SummaryExport
import org.globalforestwatch.util.Util.getAnyMapValue

object FireAlertsExport extends SummaryExport {

  override protected def exportGadm(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {

    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")
    val changeOnly: Boolean =
      getAnyMapValue[Boolean](kwargs, "changeOnly")

    val spark = summaryDF.sparkSession
    import spark.implicits._

    val numPartitions = summaryDF.rdd.getNumPartitions

    val featureCols =
      List($"featureId.iso" as "iso", $"featureId.adm1" as "adm1", $"featureId.adm2" as "adm2")

    val fireCols = _getFireCols(fireAlertType, spark)
    val cols = featureCols ++ fireCols

    val gadmDF =
      summaryDF.transform(FireAlertsDF.unpackValues(cols))
    summaryDF.unpersist()

    gadmDF.cache()

    // only export all points for viirs gadm
    if (fireAlertType == "viirs") {
      gadmDF
        .coalesce (ceil (numPartitions / 40.0).toInt)
        .write
        .options (csvOptions)
        .csv (path = outputUrl + "/all")
    }

    exportChange(gadmDF, outputUrl, numPartitions)
    if (!changeOnly) {
      exportWhitelist(gadmDF, outputUrl)
    }
    gadmDF.unpersist()
  }

  private def exportWhitelist(df: DataFrame, outputUrl: String): Unit = {
    val adm2DF = df
      .transform(FireAlertsDF.whitelist(List("iso", "adm1", "adm2")))

    adm2DF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/whitelist")

    val adm1DF = adm2DF
      .transform(FireAlertsDF.whitelist2(List("iso", "adm1")))

    adm1DF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/whitelist")

    val isoDF = adm1DF
      .transform(FireAlertsDF.whitelist2(List("iso")))

    isoDF
      .coalesce(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/whitelist")

  }

  private def exportChange(df: DataFrame, outputUrl: String, numPartitions: Int): Unit = {
    val adm2DailyDF = df
      .transform(FireAlertsDF.aggChangeDaily(List("iso", "adm1", "adm2")))

    adm2DailyDF
      .coalesce(ceil(numPartitions / 80.0).toInt)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/daily_alerts")

    val adm2DF = adm2DailyDF
      .transform(FireAlertsDF.aggChangeWeekly(List("iso", "adm1", "adm2")))

    adm2DF
      .coalesce(ceil(numPartitions / 100.0).toInt)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/weekly_alerts")

    val adm1DF = adm2DF
      .transform(FireAlertsDF.aggChangeWeekly2(List("iso", "adm1")))

    adm1DF
      .coalesce(ceil(numPartitions / 150.0).toInt)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/weekly_alerts")


    val isoDF = adm1DF
      .transform(FireAlertsDF.aggChangeWeekly2(List("iso")))


    isoDF
      .coalesce(ceil(numPartitions / 200.0).toInt)
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
      $"featureId.wdpaId" as "wdpa_protected_area__id",
      $"featureId.name" as "wdpa_protected_area__name",
      $"featureId.iucnCat" as "wdpa_protected_area__iucn_cat",
      $"featureId.iso" as "wdpa_protected_area__iso",
      $"featureId.status" as "wdpa_protected_area__status"
    )

    _export(summaryDF, outputUrl + "/wdpa", kwargs, groupByCols, unpackCols, wdpa = true)
  }

  override protected def exportFeature(summaryDF: DataFrame,
                                       outputUrl: String,
                                       kwargs: Map[String, Any]): Unit = {

    val spark = summaryDF.sparkSession
    import spark.implicits._

    val groupByCols = List("feature__id")
    val unpackCols = List($"featureId.featureId" as "feature__id")

    _export(summaryDF, outputUrl + "/feature", kwargs, groupByCols, unpackCols)
  }

  override protected def exportGeostore(summaryDF: DataFrame,
                                        outputUrl: String,
                                        kwargs: Map[String, Any]): Unit = {

    val spark = summaryDF.sparkSession
    import spark.implicits._


    val groupByCols = List("geostore__id")
    val unpackCols = List($"featureId.geostoreId" as "geostore__id")

    _export(summaryDF, outputUrl + "/geostore", kwargs, groupByCols, unpackCols)

  }

  private def _export(summaryDF: DataFrame,
                      outputUrl: String,
                      kwargs: Map[String, Any],
                      groupByCols: List[String],
                      unpackCols: List[Column],
                      wdpa: Boolean = false): Unit = {

    val changeOnly: Boolean = getAnyMapValue[Boolean](kwargs, "changeOnly")
    val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")

    val spark = summaryDF.sparkSession

    val cols = groupByCols
    val fireCols = _getFireCols(fireAlertType, spark)
    val unpackAllCols = unpackCols ++ fireCols

    val df = summaryDF.transform(
      FireAlertsDF.unpackValues(unpackAllCols, wdpa = wdpa)
    )

    val numPartitions = summaryDF.rdd.getNumPartitions

    df.cache()
    // for now only export VIIRS GADM all
//    df.coalesce(ceil(numPartitions / 40.0).toInt)
//      .write
//      .options(csvOptions)
//      .csv(path = outputUrl + "/all")

    if (!changeOnly) {
      df.transform(FireAlertsDF.whitelist(cols, wdpa = wdpa))
        .coalesce(1)
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/whitelist")
    }

    df.transform(FireAlertsDF.aggChangeDaily(cols, wdpa = wdpa))
      .coalesce(ceil(numPartitions / 100.0).toInt)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/daily_alerts")

    df.transform(FireAlertsDF.aggChangeWeekly(cols, wdpa = wdpa))
      .coalesce(ceil(numPartitions / 150.0).toInt)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/weekly_alerts")

    df.unpersist()
    ()
  }

  private def _getFireCols(fireAlertType: String, spark: SparkSession): List[Column] = {
    import spark.implicits._

    fireAlertType match {
      case "viirs" => List(
        $"fireId.lon" as "longitude",
        $"fireId.lat" as "latitude",
        $"fireId.alertDate" as "alert__date",
        $"fireId.alertTime" as "alert__time_utc",
        $"fireId.confidence" as "confidence__cat",
        $"fireId.brightTi4" as "bright_ti4__K",
        $"fireId.brightTi5" as "bright_ti5__K",
        $"fireId.frp" as "frp__MW"
      )
      case "modis" => List(
        $"fireId.lon" as "longitude",
        $"fireId.lat" as "latitude",
        $"fireId.alertDate" as "alert__date",
        $"fireId.alertTime" as "alert__time_utc",
        $"fireId.confidencePerc" as "confidence__perc",
        $"fireId.confidenceCat" as "confidence__cat",
        $"fireId.brightness" as "brightness__K",
        $"fireId.brightT31" as "bright_t31__K",
        $"fireId.frp" as "frp__MW"
      )
    }
  }
}
