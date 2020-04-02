package org.globalforestwatch.summarystats.carbonflux_custom_area

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.summarystats.SummaryExport
import org.globalforestwatch.util.Util.getAnyMapValue

object CarbonCustomExport extends SummaryExport {
  override protected def exportGadm(df: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {


    val changeOnly: Boolean =
      getAnyMapValue[Boolean](kwargs, "changeOnly")

    val exportDF = df
      .transform(CarbonCustomDF.unpackValues)

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
      .transform(CarbonCustomDF.whitelist(List("iso", "adm1", "adm2")))
      .coalesce(1)

    adm2ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/whitelist")

    val adm1ApiDF = adm2ApiDF
      .transform(CarbonCustomDF.whitelist2(List("iso", "adm1")))
      .coalesce(1)

    adm1ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/whitelist")

    val isoApiDF = adm1ApiDF
      .transform(CarbonCustomDF.whitelist2(List("iso")))
      .coalesce(1)

    isoApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/whitelist")

  }

  private def exportSummary(df: DataFrame, outputUrl: String): Unit = {

    val adm2ApiDF = df
      .transform(CarbonCustomDF.aggSummary(List("iso", "adm1", "adm2")))
      .coalesce(40) // this should result in an avg file size of 50MB. We try to keep filesize small due to memory issues


    adm2ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/summary")

    val adm1ApiDF = adm2ApiDF
      .transform(CarbonCustomDF.aggSummary2(List("iso", "adm1")))
      .coalesce(12) // this should result in an avg file size of 50MB. We try to keep filesize small due to memory issues


    adm1ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/summary")

    val isoApiDF = adm1ApiDF
      .transform(CarbonCustomDF.aggSummary2(List("iso")))
      .coalesce(4) // this should result in an avg file size of 50MB. We try to keep filesize small due to memory issues


    isoApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/summary")

  }

  private def exportChange(df: DataFrame, outputUrl: String): Unit = {
    implicit val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val adm2ApiDF = df
      .filter($"treecover_loss__year".isNotNull && $"treecover_loss__ha" > 0)
      .transform(CarbonCustomDF.aggChange(List("iso", "adm1", "adm2")))
      .coalesce(100) // this should result in an avg file size of 50MB. We try to keep filesize small due to memory issues


    adm2ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/change")

    val adm1ApiDF = adm2ApiDF
      .transform(CarbonCustomDF.aggChange(List("iso", "adm1")))
      .coalesce(30) // this should result in an avg file size of 50MB. We try to keep filesize small due to memory issues


    adm1ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/change")

    val isoApiDF = adm1ApiDF
      .transform(CarbonCustomDF.aggChange(List("iso")))
      .coalesce(10) // this should result in an avg file size of 50MB. We try to keep filesize small due to memory issues


    isoApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/change")
  }

}
