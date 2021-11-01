package org.globalforestwatch.summarystats.forest_change_diagnostic

import org.apache.spark.sql.{DataFrame, SaveMode}
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

  override def export(featureType: String,
                      summaryDF: DataFrame,
                      outputUrl: String,
                      kwargs: Map[String, Any]): Unit = {

    featureType match {

      case "gfwpro" | "wdpa" | "gadm" => exportFinal(summaryDF, outputUrl, kwargs)
      case "intermediate" => exportIntermediateList(
        summaryDF,
        outputUrl,
        kwargs.getOrElse("overwriteOutput", false).asInstanceOf[Boolean]
      )
      case _ =>
        throw new IllegalArgumentException(
          "Feature type must be one of 'gfwpro', 'intermediate', 'wdpa', or 'gadm'"
        )
    }
  }

  protected def exportFinal(summaryDF: DataFrame,
                            outputUrl: String,
                            kwargs: Map[String, Any]): Unit = {

    val writer =
      if (kwargs.getOrElse("overwriteOutput", false).asInstanceOf[Boolean])
        summaryDF.repartition(1).write.mode(SaveMode.Overwrite)
      else
        summaryDF.repartition(1).write

    writer
      .options(csvOptions)
      .csv(path = outputUrl + "/final")

  }

  private def exportIntermediateList(intermediateListDF: DataFrame,
                                     outputUrl: String,
                                     overwrite: Boolean): Unit = {

    val writer =
      if (overwrite)
        intermediateListDF.repartition(1).write.mode(SaveMode.Overwrite)
      else
        intermediateListDF.repartition(1).write

    writer
      .options(csvOptions)
      .csv(path = outputUrl + "/intermediate")
  }

}
