package org.globalforestwatch.summarystats.annualupdate

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.summarystats.SummaryExport

object AnnualUpdateExport extends SummaryExport {

  override protected def exportGadm(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {

    val exportDF = summaryDF
      .transform(AnnualUpdateDF.unpackValues)

    exportDF.cache()

    exportSummary(exportDF, outputUrl)
    exportChange(exportDF, outputUrl)

    exportDF.unpersist()

  }

  private def exportSummary(df: DataFrame, outputUrl: String): Unit = {

    val adm2ApiDF = df
      .transform(AnnualUpdateDF.aggSummary(List("iso", "adm1", "adm2")))
      .coalesce(380)

    adm2ApiDF
      // this should result in an avg file size of 100MB
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/summary")

    val adm1ApiDF = adm2ApiDF
      .transform(AnnualUpdateDF.aggSummary2(List("iso", "adm1")))
      .coalesce(190) // this should result in an avg file size of 100MB

    adm1ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/summary")

    val isoApiDF = adm1ApiDF
      .transform(AnnualUpdateDF.aggSummary2(List("iso")))
      .coalesce(135) // this should result in an avg file size of 100MB

    isoApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/summary")

  }

  private def exportChange(df: DataFrame, outputUrl: String): Unit = {
    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val adm2ApiDF = df
      .filter($"umd_tree_cover_loss__year".isNotNull && $"umd_tree_cover_loss__ha" > 0)
      .transform(AnnualUpdateDF.aggChange(List("iso", "adm1", "adm2")))
      .coalesce(670) // this should result in an avg file size of 100MB

    adm2ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm2/change")

    val adm1ApiDF = adm2ApiDF
      .transform(AnnualUpdateDF.aggChange(List("iso", "adm1")))
      .coalesce(390) // this should result in an avg file size of 100MB

    adm1ApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/adm1/change")

    val isoApiDF = adm1ApiDF
      .transform(AnnualUpdateDF.aggChange(List("iso")))
      .coalesce(270) // this should result in an avg file size of 100MB

    isoApiDF.write
      .options(csvOptions)
      .csv(path = outputUrl + "/iso/change")
  }
}
