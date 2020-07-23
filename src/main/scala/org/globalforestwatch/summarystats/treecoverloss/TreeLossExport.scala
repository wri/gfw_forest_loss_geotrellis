package org.globalforestwatch.summarystats.treecoverloss

import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport
import org.globalforestwatch.util.Util.getAnyMapValue

object TreeLossExport extends SummaryExport {

  override protected def exportFeature(summaryDF: DataFrame,
                                       outputUrl: String,
                                       kwargs: Map[String, Any]): Unit = {

    val spark = summaryDF.sparkSession
    import spark.implicits._

    val includePrimaryForest: Boolean =
      getAnyMapValue[Boolean](kwargs, "includePrimaryForest")

    summaryDF
      .transform(TreeLossDF.unpackValues)
      //.transform(TreeLossDF.primaryForestFilter(includePrimaryForest))
      .coalesce(50)
      .orderBy($"feature__id")
      .write
      .options(csvOptions)
      .csv(path = outputUrl)

  }

}
