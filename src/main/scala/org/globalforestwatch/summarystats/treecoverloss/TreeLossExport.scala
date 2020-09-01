package org.globalforestwatch.summarystats.treecoverloss

import cats.data.NonEmptyList
import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport
import org.globalforestwatch.util.Util.getAnyMapValue

object TreeLossExport extends SummaryExport {

  override protected def exportFeature(summaryDF: DataFrame,
                                       outputUrl: String,
                                       kwargs: Map[String, Any]): Unit = {

    val spark = summaryDF.sparkSession
    import spark.implicits._

    val maybeContextualLayers: Option[NonEmptyList[String]] =
      getAnyMapValue[Option[NonEmptyList[String]]](kwargs, "contextualLayers")

    val (includePrimaryForest, includePlantations) = {
      if (maybeContextualLayers isDefined) {
        val contextualLayers = maybeContextualLayers.toList
        (
          contextualLayers contains "is__umd_regional_primary_forest_2001",
          contextualLayers contains "is__gfw_plantations"
        )
      } else (false, false)
    }

    summaryDF
      .transform(TreeLossDF.unpackValues)
      .transform(
        TreeLossDF
          .contextualLayerFilter(includePrimaryForest, includePlantations)
      )
      .coalesce(1)
      .orderBy($"feature__id", $"umd_tree_cover_density__threshold")
      .write
      .options(csvOptions)
      .csv(path = outputUrl)

  }

}
