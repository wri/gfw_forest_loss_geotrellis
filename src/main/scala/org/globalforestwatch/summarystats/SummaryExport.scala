package org.globalforestwatch.summarystats

import org.apache.spark.sql.DataFrame

trait SummaryExport {

  val csvOptions: Map[String, String] = Map(
    "header" -> "true",
    "delimiter" -> "\t",
    //"quote" -> "\u0000",
    "escape" -> "\"",
    "quoteMode" -> "MINIMAL",
    "nullValue" -> null,
    "emptyValue" -> null
  )

  def export(featureType: String,
             summaryDF: DataFrame,
             outputUrl: String,
             kwargs: Map[String, Any]
             ): Unit = {

    featureType match {
      case "gadm" => exportGadm(summaryDF, outputUrl, kwargs)
      case "feature" => exportFeature(summaryDF, outputUrl, kwargs)
      case "wdpa" => exportWdpa(summaryDF, outputUrl, kwargs)
      case "geostore" => exportGeostore(summaryDF, outputUrl, kwargs)
      case "gfwpro" => exportGfwPro(summaryDF, outputUrl, kwargs)
      case _ =>
        throw new IllegalArgumentException(
          "Feature type must be one of 'gadm', 'wdpa', 'geostore' 'gfwpro' or 'feature'"
        )
    }
  }

  protected def exportGadm(summaryDF: DataFrame,
                           outputUrl: String,
                           kwargs: Map[String, Any]): Unit = throw new NotImplementedError("GADM feature analysis not implemented")

  protected def exportFeature(summaryDF: DataFrame,
                              outputUrl: String,
                              kwargs: Map[String, Any]): Unit = throw new NotImplementedError("Simple feature analysis not implemented")

  protected def exportWdpa(summaryDF: DataFrame,
                           outputUrl: String,
                           kwargs: Map[String, Any]): Unit = throw new NotImplementedError("WDPA feature analysis not implemented")

  protected def exportGeostore(summaryDF: DataFrame,
                               outputUrl: String,
                               kwargs: Map[String, Any]): Unit = throw new NotImplementedError("Geostore feature analysis not implemented")

  protected def exportGfwPro(summaryDF: DataFrame,
                             outputUrl: String,
                             kwargs: Map[String, Any]): Unit = throw new NotImplementedError("GfwPro feature analysis not implemented")
}
