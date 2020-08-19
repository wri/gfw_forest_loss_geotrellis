package org.globalforestwatch.summarystats.carbonflux

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.summarystats.SummaryExport
import org.globalforestwatch.util.Util.getAnyMapValue

object CarbonFluxExport extends SummaryExport {

  override protected def exportGadm(df: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {



    val changeOnly: Boolean =
      getAnyMapValue[Boolean](kwargs, "changeOnly")

    val exportDF = df
      .transform(CarbonFluxDF.unpackValues)

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
      .transform(CarbonFluxDF.whitelist(List("iso", "adm1", "adm2")))
      .coalesce(1)

    adm2ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/whitelist")

    val adm1ApiDF = adm2ApiDF
      .transform(CarbonFluxDF.whitelist2(List("iso", "adm1")))
      .coalesce(1)

    adm1ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/whitelist")

    val isoApiDF = adm1ApiDF
      .transform(CarbonFluxDF.whitelist2(List("iso")))
      .coalesce(1)

    isoApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/whitelist")

  }

  private def exportSummary(df: DataFrame, outputUrl: String): Unit = {

    val adm2ApiDF = df
      .transform(CarbonFluxDF.aggSummary(List("iso", "adm1", "adm2")))
      .coalesce(80) // this should result in an avg file size of 50MB. We try to keep filesize small due to memory issues


    adm2ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/summary")

    val adm1ApiDF = adm2ApiDF
      .transform(CarbonFluxDF.aggSummary2(List("iso", "adm1")))
      .coalesce(24) // this should result in an avg file size of 50MB. We try to keep filesize small due to memory issues


    adm1ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/summary")

    val isoApiDF = adm1ApiDF
      .transform(CarbonFluxDF.aggSummary2(List("iso")))
      .coalesce(8) // this should result in an avg file size of 50MB. We try to keep filesize small due to memory issues


    isoApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/summary")

  }

  private def exportChange(df: DataFrame, outputUrl: String): Unit = {
    implicit val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val adm2ApiDF = df
      .filter($"umd_tree_cover_loss__year".isNotNull && $"umd_tree_cover_loss__ha" > 0)
      .transform(CarbonFluxDF.aggChange(List("iso", "adm1", "adm2")))
      .coalesce(200) // this should result in an avg file size of 50MB. We try to keep filesize small due to memory issues


    adm2ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/change")

    val adm1ApiDF = adm2ApiDF
      .transform(CarbonFluxDF.aggChange(List("iso", "adm1")))
      .coalesce(60) // this should result in an avg file size of 50MB. We try to keep filesize small due to memory issues


    adm1ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/change")

    val isoApiDF = adm1ApiDF
      .transform(CarbonFluxDF.aggChange(List("iso")))
      .coalesce(20) // this should result in an avg file size of 50MB. We try to keep filesize small due to memory issues


    isoApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/change")
  }

}
