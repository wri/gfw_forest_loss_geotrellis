package org.globalforestwatch.summarystats.forest_change_diagnostic

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
              ForestChangeDiagnosticRowSimple(simpleId.toString,
                summary.stats.treeCoverLossYearly.toString,
                summary.stats.treeCoverLossPrimaryForestYearly.toString,
                summary.stats.treeCoverLossPeatLandYearly.toString,
                summary.stats.treeCoverLossIntactForestYearly.toString,
                summary.stats.treeCoverLossProtectedAreasYearly.toString,
                summary.stats.treeCoverLossSEAsiaLandCoverYearly.toString,
                summary.stats.treeCoverLossIDNLandCoverYearly.toString,
                summary.stats.treeCoverLossSoyPlanedAreasYearly.toString,
                summary.stats.treeCoverLossIDNForestAreaYearly.toString,
                summary.stats.treeCoverLossIDNForestMoratoriumYearly.toString,
                summary.stats.prodesLossYearly.toString,
                summary.stats.prodesLossProtectedAreasYearly.toString,
                summary.stats.prodesLossProdesPrimaryForestYearly.toString,
                summary.stats.treeCoverLossBRABiomesYearly.toString,
                summary.stats.treeCoverExtent.toString,
                summary.stats.treeCoverExtentPrimaryForest.toString,
                summary.stats.treeCoverExtentProtectedAreas.toString,
                summary.stats.treeCoverExtentPeatlands.toString,
                summary.stats.treeCoverExtentIntactForests.toString,
                summary.stats.primaryForestArea.toString,
                summary.stats.intactForest2016Area.toString,
                summary.stats.totalArea.toString,
                summary.stats.protectedAreasArea.toString,
                summary.stats.peatlandsArea.toString,
                summary.stats.braBiomesArea.toString,
                summary.stats.idnForestAreaArea.toString,
                summary.stats.seAsiaLandCoverArea.toString,
                summary.stats.idnLandCoverArea.toString,
                summary.stats.idnForestMoratoriumArea.toString

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
