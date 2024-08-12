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

    val contextualLayers: List[String] =
      getAnyMapValue[NonEmptyList[String]](kwargs, "contextualLayers").toList

    val (includePrimaryForest, includePlantations, includeGlobalPeat, includeTreeCoverLossFires, includeTreeCoverLoss, includeIntactForestLandscapes2000) = {
      (
        contextualLayers contains "is__umd_regional_primary_forest_2001",
        contextualLayers contains "is__gfw_plantations",
        contextualLayers contains "is__global_peat",
        contextualLayers contains "is__tree_cover_loss_from_fires",
        contextualLayers contains "is__tree_cover_loss",
        contextualLayers contains "is__intact_forest_landscapes_2000",
      )
    }

    val carbonPools: Boolean =
      getAnyMapValue[Boolean](kwargs, "carbonPools")

    val simpleAGBEmis: Boolean =
      getAnyMapValue[Boolean](kwargs, "simpleAGBEmis")

    val emisGasAnnual: Boolean =
      getAnyMapValue[Boolean](kwargs, "emisGasAnnual")


    summaryDF
      .transform(TreeLossDF.unpackValues(carbonPools, simpleAGBEmis, emisGasAnnual))
      .transform(TreeLossDF.contextualLayerFilter(
        includePrimaryForest, includePlantations, includeGlobalPeat,
        includeTreeCoverLossFires, includeTreeCoverLoss, includeIntactForestLandscapes2000,
        carbonPools, simpleAGBEmis, emisGasAnnual))
      .coalesce(1)
      .orderBy($"feature__id", $"umd_tree_cover_density__threshold")
      .write
      .options(csvOptions)
      .csv(path = outputUrl)

  }

}
