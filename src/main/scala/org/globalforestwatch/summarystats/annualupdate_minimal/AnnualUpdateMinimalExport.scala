package org.globalforestwatch.summarystats.annualupdate_minimal

import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport

object AnnualUpdateMinimalExport extends SummaryExport {

  override protected def exportGadm(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {

    val spark = summaryDF.sparkSession
    import spark.implicits._
    val adm2DF = summaryDF
      .transform(Adm2DF.unpackValues)

    adm2DF.repartition($"iso")
    adm2DF.cache()

    val csvOptions = Map(
      "header" -> "true",
      "delimiter" -> "\t",
      "quote" -> "\u0000",
      "quoteMode" -> "NONE",
      "nullValue" -> "\u0000"
    )

    val adm2SummaryDF = adm2DF
      .transform(Adm2SummaryDF.sumArea)

    adm2SummaryDF
      .transform(Adm2SummaryDF.roundValues)
      .coalesce(1)
      .orderBy($"country", $"subnational1", $"subnational2", $"threshold")
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/summary/adm2")

    val adm1SummaryDF = adm2SummaryDF.transform(Adm1SummaryDF.sumArea)

    adm1SummaryDF
      .transform(Adm1SummaryDF.roundValues)
      .coalesce(1)
      .orderBy($"country", $"subnational1", $"threshold")
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/summary/adm1")

    val isoSummaryDF = adm1SummaryDF.transform(IsoSummaryDF.sumArea)

    isoSummaryDF
      .transform(IsoSummaryDF.roundValues)
      .coalesce(1)
      .orderBy($"iso", $"threshold")
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/summary/iso")

    val apiDF = adm2DF
      .transform(ApiDF.setNull)

    apiDF.cache()
    //            adm2DF.unpersist()

    val adm2ApiDF = apiDF
      .transform(Adm2ApiDF.nestYearData)

    adm2ApiDF
    //.coalesce(1)
    //            .repartition(
    //            outputPartitionCount,
    //            $"iso",
    //            $"adm1",
    //            $"adm2",
    //            $"threshold"
    //          )
    //            .orderBy($"iso", $"adm1", $"adm2", $"threshold")
    .toJSON
      .mapPartitions(vals => Iterator("[" + vals.mkString(",") + "]"))
      .write
      .text(outputUrl + "/api/adm2")

    val tempApiDF = apiDF
      .transform(Adm1ApiDF.sumArea)

    tempApiDF.cache()
    apiDF.unpersist()

    val adm1ApiDF = tempApiDF
      .transform(Adm1ApiDF.nestYearData)

    adm1ApiDF
    //            .repartition(outputPartitionCount, $"iso", $"adm1", $"threshold")
    //            .orderBy($"iso", $"adm1", $"threshold")
    .toJSON
      .mapPartitions(vals => Iterator("[" + vals.mkString(",") + "]"))
      .write
      .text(outputUrl + "/api/adm1")

    val isoApiDF = tempApiDF
      .transform(IsoApiDF.sumArea)
      .transform(IsoApiDF.nestYearData)

    isoApiDF
    //            .repartition(outputPartitionCount, $"iso", $"threshold")
    //            .orderBy($"iso", $"threshold")
    .toJSON
      .mapPartitions(vals => Iterator("[" + vals.mkString(",") + "]"))
      .write
      .text(outputUrl + "/api/iso")

    tempApiDF.unpersist()

  }

}
