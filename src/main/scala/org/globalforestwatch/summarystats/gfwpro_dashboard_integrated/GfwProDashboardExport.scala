package org.globalforestwatch.summarystats.gfwpro_dashboard_integrated

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.globalforestwatch.summarystats.SummaryExport
import org.globalforestwatch.util.Util.getAnyMapValue

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
    val saveMode =
      if (getAnyMapValue[Boolean](kwargs, "overwriteOutput"))
        SaveMode.Overwrite
      else
        SaveMode.ErrorIfExists

    summaryDF
      .repartition(1)
      .write
      .mode(saveMode)
      .options(csvOptions)
      .csv(path = outputUrl + "/final")
  }

}
