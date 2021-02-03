package org.globalforestwatch.summarystats.forest_change_diagnostic

import io.circe.syntax._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, SimpleFeatureId}

case class ForestChangeDiagnosticDFFactory(
  featureType: String,
  summaryRDD: RDD[(FeatureId, ForestChangeDiagnosticSummary)],
  spark: SparkSession
) {

  import spark.implicits._

  def getDataFrame: DataFrame = {
    featureType match {
      case "feature" => getFeatureDataFrame
      case _ =>
        throw new IllegalArgumentException("Not a valid FeatureId")
    }
  }

  private def getFeatureDataFrame: DataFrame = {
    summaryRDD
      .map {
        case (id, summary) =>
          id match {
            case simpleId: SimpleFeatureId =>
              ForestChangeDiagnosticRowSimple(simpleId.featureId.asJson.noSpaces,
                summary.stats.treeCoverLossYearly.toJson,
                summary.stats.treeCoverLossPrimaryForestYearly.toJson,
                summary.stats.treeCoverLossPeatLandYearly.toJson,
                summary.stats.treeCoverLossIntactForestYearly.toJson,
                summary.stats.treeCoverLossProtectedAreasYearly.toJson,
                summary.stats.treeCoverLossSEAsiaLandCoverYearly.toJson,
                summary.stats.treeCoverLossIDNLandCoverYearly.toJson,
                summary.stats.treeCoverLossSoyPlanedAreasYearly.toJson,
                summary.stats.treeCoverLossIDNForestAreaYearly.toJson,
                summary.stats.treeCoverLossIDNForestMoratoriumYearly.toJson,
                summary.stats.prodesLossYearly.toJson,
                summary.stats.prodesLossProtectedAreasYearly.toJson,
                summary.stats.prodesLossProdesPrimaryForestYearly.toJson,
                summary.stats.treeCoverLossBRABiomesYearly.toJson,
                summary.stats.treeCoverExtent.toJson,
                summary.stats.treeCoverExtentPrimaryForest.toJson,
                summary.stats.treeCoverExtentProtectedAreas.toJson,
                summary.stats.treeCoverExtentPeatlands.toJson,
                summary.stats.treeCoverExtentIntactForests.toJson,
                summary.stats.primaryForestArea.toJson,
                summary.stats.intactForest2016Area.toJson,
                summary.stats.totalArea.toJson,
                summary.stats.protectedAreasArea.toJson,
                summary.stats.peatlandsArea.toJson,
                summary.stats.braBiomesArea.toJson,
                summary.stats.idnForestAreaArea.toJson,
                summary.stats.seAsiaLandCoverArea.toJson,
                summary.stats.idnLandCoverArea.toJson,
                summary.stats.idnForestMoratoriumArea.toJson

              )
            case _ =>
              throw new IllegalArgumentException("Not a SimpleFeatureId")
          }
      }
      .toDF("id",
        "treeCoverLossYearly",
        "treeCoverLossPrimaryForestYearly",
        "treeCoverLossPeatLandYearly",
        "treeCoverLossIntactForestYearly",
        "treeCoverLossProtectedAreasYearly",
        "treeCoverLossSEAsiaLandCoverYearly",
        "treeCoverLossIDNLandCoverYearly",
        "treeCoverLossSoyPlanedAreasYearly",
        "treeCoverLossIDNForestAreaYearly",
        "treeCoverLossIDNForestMoratoriumYearly",
        "prodesLossYearly",
        "prodesLossProtectedAreasYearly",
        "prodesLossProdesPrimaryForestYearly",
        "treeCoverLossBRABiomesYearly",
        "treeCoverExtent",
        "treeCoverExtentPrimaryForest",
        "treeCoverExtentProtectedAreas",
        "treeCoverExtentPeatlands",
        "treeCoverExtentIntactForests",
        "primaryForestArea",
        "intactForest2016Area",
        "totalArea",
        "protectedAreasArea",
        "peatlandsArea",
        "braBiomesArea",
        "idnForestAreaArea",
        "seAsiaLandCoverArea",
        "idnLandCoverArea",
        "idnForestMoratoriumArea")
    
  }

}
