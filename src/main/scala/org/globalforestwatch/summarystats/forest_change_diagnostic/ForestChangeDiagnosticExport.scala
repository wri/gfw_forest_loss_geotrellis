package org.globalforestwatch.summarystats.forest_change_diagnostic

import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport

object ForestChangeDiagnosticExport extends SummaryExport {

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
