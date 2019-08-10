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

    val thresholdFilter: Seq[Int] =
      getAnyMapValue[Seq[Int]](kwargs, "thresholdFilter")
    val includePrimaryForest: Boolean =
      getAnyMapValue[Boolean](kwargs, "includePrimaryForest")

    summaryDF
      .filter($"data_group.threshold".isin(thresholdFilter: _*))
      .transform(TreeLossDF.unpackValues)
      .transform(TreeLossDF.primaryForestFilter(includePrimaryForest))
      .coalesce(1)
      .orderBy($"feature_id", $"threshold")
      .write
      .options(csvOptions)
      .csv(path = outputUrl)

  }

}
