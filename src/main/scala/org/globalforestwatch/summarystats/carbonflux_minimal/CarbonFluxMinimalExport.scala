package org.globalforestwatch.summarystats.carbonflux_minimal

import cats.data.NonEmptyList
import org.apache.spark.sql.DataFrame
import org.globalforestwatch.summarystats.SummaryExport
import org.globalforestwatch.util.Util.getAnyMapValue

object CarbonFluxMinimalExport extends SummaryExport {

  override protected def exportFeature(summaryDF: DataFrame,
                                       outputUrl: String,
                                       kwargs: Map[String, Any]): Unit = {

    val spark = summaryDF.sparkSession
    import spark.implicits._

    val contextualLayers: List[String] =
      getAnyMapValue[NonEmptyList[String]](kwargs, "contextualLayers").toList

    val (includePrimaryForest, includePlantations) = {
      (
        contextualLayers contains "is__umd_regional_primary_forest_2001",
        contextualLayers contains "is__gfw_plantations"
      )
    }

    summaryDF
      .transform(CarbonFluxMinimalDF.unpackValues)
      .transform(
        CarbonFluxMinimalDF
          .contextualLayerFilter(includePrimaryForest, includePlantations)
      )
      .coalesce(1)
      .orderBy($"feature__id", $"umd_tree_cover_density__threshold")
      .write
      .options(csvOptions)
      .csv(path = outputUrl)

  }

}
