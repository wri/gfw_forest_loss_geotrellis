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
                summary.stats.idnForestMoratoriumArea.toJson,
                summary.stats.southAmericaPresence.toJson,
                summary.stats.legalAmazonPresence.toJson,
                summary.stats.braBiomesPresence.toJson,
                summary.stats.cerradoBiomesPresence.toJson,
                summary.stats.seAsiaPresence.toJson,
                summary.stats.idnPresence.toJson

              )
            case _ =>
              throw new IllegalArgumentException("Not a SimpleFeatureId")
          }
      }
      .toDF("location_id",

        "tree_cover_loss_total_yearly", // treeCoverLossYearly
        "tree_cover_loss_primary_forest_yearly", // treeCoverLossPrimaryForestYearly
        "tree_cover_loss_peat_yearly", //treeCoverLossPeatLandYearly
        "tree_cover_loss_intact_forest_yearly", // treeCoverLossIntactForestYearly
        "tree_cover_loss_protected_areas_yearly", // treeCoverLossProtectedAreasYearly
        "tree_cover_loss_sea_landcover_yearly", // treeCoverLossSEAsiaLandCoverYearly
        "tree_cover_loss_idn_landcover_yearly", // treeCoverLossIDNLandCoverYearly
        "tree_cover_loss_soy_yearly", // treeCoverLossSoyPlanedAreasYearly
        "tree_cover_loss_idn_legal_yearly", // treeCoverLossIDNForestAreaYearly
        "tree_cover_loss_idn_forest_moratorium_yearly", // treeCoverLossIDNForestMoratoriumYearly
        "tree_cover_loss_prodes_yearly", // prodesLossYearly
        "tree_cover_loss_prodes_wdpa_yearly", // prodesLossProtectedAreasYearly
        "tree_cover_loss_prodes_primary_forest_yearly", // prodesLossProdesPrimaryForestYearly
        "tree_cover_loss_brazil_biomes_yearly", // treeCoverLossBRABiomesYearly
        "tree_cover_extent_total", // treeCoverExtent
        "tree_cover_extent_primary_forest", // treeCoverExtentPrimaryForest
        "tree_cover_extent_protected_areas", // treeCoverExtentProtectedAreas
        "tree_cover_extent_peat", // treeCoverExtentPeatlands
        "tree_cover_extent_intact_forest", // treeCoverExtentIntactForests
        "natural_habitat_primary", // primaryForestArea
        "natural_habitat_intact_forest", //intactForest2016Area
        "total_area", // totalArea
        "protected_areas_area", // protectedAreasArea
        "peat_area", // peatlandsArea
        "brazil_biomes", // braBiomesArea
        "idn_legal_area", // idnForestAreaArea
        "sea_landcover_area", // seAsiaLandCoverArea
        "idn_landcover_area", // idnLandCoverArea
        "idn_forest_moratorium_area", // idnForestMoratoriumArea
        "south_america_presence", // southAmericaPresence,
        "legal_amazon_presence", // legalAmazonPresence,
        "brazil_biomes_presence", // braBiomesPresence,
        "cerrado_biome_presence", // cerradoBiomesPresence,
        "southeast_asia_presence", // seAsiaPresence,
        "indonesia_presence", // idnPresence
      )
    
  }

}
