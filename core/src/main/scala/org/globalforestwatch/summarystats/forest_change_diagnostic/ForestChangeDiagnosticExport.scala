package org.globalforestwatch.summarystats.forest_change_diagnostic

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.globalforestwatch.summarystats.SummaryExport
import org.globalforestwatch.util.Util.getAnyMapValue

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

  override def export(
    featureType: String,
    summaryDF: DataFrame,
    outputUrl: String,
    kwargs: Map[String, Any]
  ): Unit = {
    val saveMode: SaveMode =
      if (getAnyMapValue[Boolean](kwargs, "overwriteOutput"))
        SaveMode.Overwrite
      else
        SaveMode.ErrorIfExists

    featureType match {
      case "gfwpro" | "wdpa" | "gadm" =>
        summaryDF
          .repartition(1)
          .write
          .mode(saveMode)
          .options(csvOptions)
          .csv(path = outputUrl + "/final")

      case "intermediate" =>
        summaryDF
          .repartition(1)
          .write
          .mode(saveMode)
          .options(csvOptions)
          .csv(path = outputUrl + "/intermediate")

      case _ =>
        throw new IllegalArgumentException(
          "Feature type must be one of 'gfwpro', 'intermediate', 'wdpa', or 'gadm'"
        )
    }
  }
}
