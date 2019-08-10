package org.globalforestwatch.summarystats

import org.apache.spark.sql.DataFrame

trait SummaryExport {

  val csvOptions: Map[String, String] = Map(
    "header" -> "true",
    "delimiter" -> ",",
    "quote" -> "\u0000",
    "quoteMode" -> "NONE",
    "nullValue" -> "\u0000"
  )

  def export(featureType: String,
             summaryDF: DataFrame,
             outputUrl: String,
             kwargs: Map[String, Any]): Unit = {

    featureType match {
      case "gadm"    => exportGadm(summaryDF, outputUrl, kwargs)
      case "feature" => exportFeature(summaryDF, outputUrl, kwargs)
      case "wdpa"    => exportWdpa(summaryDF, outputUrl, kwargs)
      case _ =>
        throw new IllegalArgumentException(
          "Feature type must be one of 'gadm' and 'feature'"
        )
    }
  }

  protected def exportGadm(summaryDF: DataFrame,
                           outputUrl: String,
                           kwargs: Map[String, Any]): Unit =
    throw new UnsupportedOperationException(
      "This feature type is not yet implemented"
    )

  protected def exportFeature(summaryDF: DataFrame,
                              outputUrl: String,
                              kwargs: Map[String, Any]): Unit =
    throw new UnsupportedOperationException(
      "This feature type is not yet implemented"
    )

  protected def exportWdpa(summaryDF: DataFrame,
                           outputUrl: String,
                           kwargs: Map[String, Any]): Unit =
    throw new UnsupportedOperationException(
      "This feature type is not yet implemented"
    )
}
