package org.globalforestwatch.summarystats.carbonflux

import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport

object CarbonFluxExport extends SummaryExport {


  override protected def exportGadm(df: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {

    def exportSummary(df: DataFrame): Unit = {

      val adm2ApiDF = df.transform(Adm2ApiDF.sumArea)
      adm2ApiDF
        .coalesce(40) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm2/summary")

      val adm1ApiDF = adm2ApiDF.transform(Adm1ApiDF.sumArea)
      adm1ApiDF
        .coalesce(12) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm1/summary")

      val isoApiDF = adm1ApiDF.transform(IsoApiDF.sumArea)
      isoApiDF
        .coalesce(4) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/iso/summary")

    }

    def exportChange(df: DataFrame): Unit = {
      val adm2ApiDF = df.transform(Adm2ApiDF.sumChange)
      adm2ApiDF
        .coalesce(100) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm2/change")

      val adm1ApiDF = adm2ApiDF.transform(Adm1ApiDF.sumChange)
      adm1ApiDF
        .coalesce(30) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm1/change")

      val isoApiDF = adm1ApiDF.transform(IsoApiDF.sumChange)
      isoApiDF
        .coalesce(10) // this should result in an avg file size of 100MB
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/iso/change")
    }

    val exportDF = df
      .transform(ApiDF.unpackValues)

    exportDF.cache()

    exportSummary(exportDF)
    exportChange(exportDF)

    exportDF.unpersist()

  }

}
