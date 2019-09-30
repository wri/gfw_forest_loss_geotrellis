package org.globalforestwatch.summarystats.annualupdate

import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport

object AnnualUpdateExport extends SummaryExport {

  override protected def exportGadm(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {

    def exportSummary(df: DataFrame): Unit = {

      val adm2ApiDF = df.transform(Adm2ApiDF.sumArea)
      adm2ApiDF
        .coalesce(380) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm2/summary")

      val adm1ApiDF = adm2ApiDF.transform(Adm1ApiDF.sumArea)
      adm1ApiDF
        .coalesce(190) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm1/summary")

      val isoApiDF = adm1ApiDF.transform(IsoApiDF.sumArea)
      isoApiDF
        .coalesce(135) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/iso/summary")

    }

    def exportChange(df: DataFrame): Unit = {
      val adm2ApiDF = df.transform(Adm2ApiDF.sumChange)
      adm2ApiDF
        .coalesce(670) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm2/change")

      val adm1ApiDF = adm2ApiDF.transform(Adm1ApiDF.sumChange)
      adm1ApiDF
        .coalesce(390) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm1/change")

      val isoApiDF = adm1ApiDF.transform(IsoApiDF.sumChange)
      isoApiDF
        .coalesce(270) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/iso/change")
    }

    val exportDF = summaryDF
      .transform(ApiDF.unpackValues)

    exportDF.cache()

    exportSummary(exportDF)
    exportChange(exportDF)

    exportDF.unpersist()

  }

}
