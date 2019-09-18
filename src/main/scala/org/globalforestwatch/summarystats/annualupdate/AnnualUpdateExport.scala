package org.globalforestwatch.summarystats.annualupdate

import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport

object AnnualUpdateExport extends SummaryExport {

  override protected def exportGadm(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {

    val spark = summaryDF.sparkSession
    import spark.implicits._

    def exportSummary(df: DataFrame): Unit = {

      val adm2SummaryDF = df
        .transform(Adm2SummaryDF.sumArea)

      adm2SummaryDF
        .transform(Adm2SummaryDF.roundValues)
        .coalesce(1)
        .orderBy($"country", $"subnational1", $"subnational2", $"threshold")
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm2/summary")

      val adm1SummaryDF = adm2SummaryDF.transform(Adm1SummaryDF.sumArea)

      adm1SummaryDF
        .transform(Adm1SummaryDF.roundValues)
        .coalesce(1)
        .orderBy($"country", $"subnational1", $"threshold")
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/adm1/summary")

      val isoSummaryDF = adm1SummaryDF.transform(IsoSummaryDF.sumArea)

      isoSummaryDF
        .transform(IsoSummaryDF.roundValues)
        .coalesce(1)
        .orderBy($"country", $"threshold")
        .write
        .options(csvOptions)
        .csv(path = outputUrl + "/iso/summary")


    }

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
    exportSummary(exportDF)

    exportDF.unpersist()


  }

}
