package org.globalforestwatch.summarystats.gfwpro_dashboard

import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport

object GfwProDashboardExport extends SummaryExport {

  override val csvOptions: Map[String, String] = Map(
    "header" -> "true",
    "delimiter" -> "\t",
    "quote" -> "\u0000",
    "escape" -> "\u0000",
    "quoteMode" -> "NONE",
    "nullValue" -> null,
    "emptyValue" -> null
  )

  override protected def exportGfwPro(summaryDF: DataFrame,
                                      outputUrl: String,
                                      kwargs: Map[String, Any]): Unit = {

    summaryDF
      .repartition(1)
      .write
      .options(csvOptions)
      .csv(path = outputUrl + "/final")

  }

}
