package org.globalforestwatch.summarystats.carbon_sensitivity

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.summarystats.SummaryExport
import org.globalforestwatch.util.Util.getAnyMapValue

object CarbonSensitivityExport extends SummaryExport {
  override protected def exportGadm(df: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {


    val changeOnly: Boolean =
      getAnyMapValue[Boolean](kwargs, "changeOnly")

    val exportDF = df
      .transform(CarbonSensitivityDF.unpackValues)

    exportDF.cache()

    if (!changeOnly) {
      exportWhitelist(exportDF, outputUrl)
      exportSummary(exportDF, outputUrl)
    }
    exportChange(exportDF, outputUrl)

    exportDF.unpersist()

  }

  private def exportWhitelist(df: DataFrame, outputUrl: String): Unit = {

    val adm2ApiDF = df
      .transform(CarbonSensitivityDF.whitelist(List("iso", "adm1", "adm2")))
      .coalesce(1)

    adm2ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/whitelist")

    val adm1ApiDF = adm2ApiDF
      .transform(CarbonSensitivityDF.whitelist2(List("iso", "adm1")))
      .coalesce(1)

    adm1ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/whitelist")

    val isoApiDF = adm1ApiDF
      .transform(CarbonSensitivityDF.whitelist2(List("iso")))
      .coalesce(1)

    isoApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/whitelist")

  }

  private def exportSummary(df: DataFrame, outputUrl: String): Unit = {

    val adm2ApiDF = df
      .transform(CarbonSensitivityDF.aggSummary(List("iso", "adm1", "adm2")))
      .coalesce(80) // this should result in an avg file size of 100MB.

    adm2ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/summary")

    val adm1ApiDF = adm2ApiDF
      .transform(CarbonSensitivityDF.aggSummary2(List("iso", "adm1")))
      .coalesce(24) // this should result in an avg file size of 100MB.

    adm1ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/summary")

    val isoApiDF = adm1ApiDF
      .transform(CarbonSensitivityDF.aggSummary2(List("iso")))
      .coalesce(8) // this should result in an avg file size of 100MB.

    isoApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/summary")

  }

  private def exportChange(df: DataFrame, outputUrl: String): Unit = {
    implicit val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val adm2ApiDF = df
      .filter($"umd_tree_cover_loss__year".isNotNull && $"umd_tree_cover_loss__ha" > 0)
      .transform(CarbonSensitivityDF.aggChange(List("iso", "adm1", "adm2")))
      .coalesce(200) // this should result in an avg file size of 100MB.

    adm2ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/change")

    val adm1ApiDF = adm2ApiDF
      .transform(CarbonSensitivityDF.aggChange(List("iso", "adm1")))
      .coalesce(60) // this should result in an avg file size of 100MB.

    adm1ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/change")

    val isoApiDF = adm1ApiDF
      .transform(CarbonSensitivityDF.aggChange(List("iso")))
      .coalesce(20) // this should result in an avg file size of 100MB.

    isoApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/change")
  }

}
