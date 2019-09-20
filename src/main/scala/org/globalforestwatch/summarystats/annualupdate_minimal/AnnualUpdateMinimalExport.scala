package org.globalforestwatch.summarystats.annualupdate_minimal

import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport
import org.globalforestwatch.summarystats.annualupdate.AnnualUpdateExport

object AnnualUpdateMinimalExport extends SummaryExport {

  override protected def exportGadm(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {


    def exportArea(df: DataFrame): Unit = {

      val adm2ApiDF = df.transform(Adm2ApiDF.sumArea)
      adm2ApiDF.write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm2/area")

      val adm1ApiDF = adm2ApiDF.transform(Adm1ApiDF.sumArea)
      adm1ApiDF.write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm1/area")

      val isoApiDF = adm1ApiDF.transform(IsoApiDF.sumArea)
      isoApiDF.write
        .options(csvOptions)
        .csv(path = outputUrl + "/iso/area")

    }

    def exportChange(df: DataFrame): Unit = {
      val adm2ApiDF = df.transform(Adm2ApiDF.sumChange)
      adm2ApiDF.write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm2/change")

      val adm1ApiDF = adm2ApiDF.transform(Adm1ApiDF.sumChange)
      adm1ApiDF.write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm1/change")

      val isoApiDF = adm1ApiDF.transform(IsoApiDF.sumChange)
      isoApiDF.write
        .options(csvOptions)
        .csv(path = outputUrl + "/iso/change")
    }

    val exportDF = summaryDF
      .transform(ApiDF.unpackValues)
    //          apiDF.repartition($"iso")
    //    val apiDF =
    //    adm2DF
    //      .transform(ApiDF.setNull)
    exportDF.cache()

    exportArea(exportDF)
    exportChange(exportDF)
    AnnualUpdateExport.exportSummary(exportDF, outputUrl)

    exportDF.unpersist()


  }

}