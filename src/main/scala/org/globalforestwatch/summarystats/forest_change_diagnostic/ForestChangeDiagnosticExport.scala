package org.globalforestwatch.summarystats.forest_change_diagnostic

import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport

object ForestChangeDiagnosticExport extends SummaryExport {

  override val csvOptions: Map[String, String] = Map(
    "header" -> "true",
    "delimiter" -> "\t",
    "quote" -> "\u0000",
    "escape" -> "\u0000",
    "quoteMode" -> "NONE",
    "nullValue" -> null,
    "emptyValue" -> null
  )
  override protected def exportFeature(summaryDF: DataFrame,
                                       outputUrl: String,
                                       kwargs: Map[String, Any]): Unit = {

    summaryDF
      .coalesce(1)
      .write
      //      .json(path = outputUrl)
      .options(csvOptions)
      .csv(path = outputUrl)

  }

}
