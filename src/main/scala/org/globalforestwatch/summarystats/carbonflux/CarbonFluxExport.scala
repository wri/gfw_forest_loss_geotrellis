package org.globalforestwatch.summarystats.carbonflux

import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport

object CarbonFluxExport extends SummaryExport {

  override protected def exportGadm(summaryDF: DataFrame,
                                    outputUrl: String,
                                    kwargs: Map[String, Any]): Unit = {

    val spark = summaryDF.sparkSession
    import spark.implicits._

    summaryDF
      .transform(ApiDF.unpackValues)
      // .transform(ApiDF.setNull)
      //              .coalesce(1)
      .orderBy($"iso", $"adm1", $"adm2", $"threshold")
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/summary/adm2")

  }

}
