package org.globalforestwatch.summarystats.forest_change_diagnostic

import io.circe.syntax._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, SimpleFeatureId}

case class ForestChangeDiagnosticDFFactory(
                                            featureType: String,
                                            dataRDD: RDD[(FeatureId, ForestChangeDiagnosticData)],
                                            spark: SparkSession,
                                            kwargs: Map[String, Any]
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

    dataRDD

      .map {
        case (id, data) =>
          id match {
            case simpleId: SimpleFeatureId =>
              ForestChangeDiagnosticRowSimple(
                simpleId.featureId.asJson.noSpaces,
                data.treeCoverLossTcd30Yearly.toJson,
                data.treeCoverLossPrimaryForestYearly.toJson,
                data.treeCoverLossPeatLandYearly.toJson,
                data.treeCoverLossIntactForestYearly.toJson,
                data.treeCoverLossProtectedAreasYearly.toJson,
                data.treeCoverLossSEAsiaLandCoverYearly.toJson,
                data.treeCoverLossIDNLandCoverYearly.toJson,
                data.treeCoverLossSoyPlanedAreasYearly.toJson,
                data.treeCoverLossIDNForestAreaYearly.toJson,
                data.treeCoverLossIDNForestMoratoriumYearly.toJson,
                data.prodesLossYearly.toJson,
                data.prodesLossProtectedAreasYearly.toJson,
                data.prodesLossProdesPrimaryForestYearly.toJson,
                data.treeCoverLossBRABiomesYearly.toJson,
                data.treeCoverExtent.toJson,
                data.treeCoverExtentPrimaryForest.toJson,
                data.treeCoverExtentProtectedAreas.toJson,
                data.treeCoverExtentPeatlands.toJson,
                data.treeCoverExtentIntactForests.toJson,
                data.primaryForestArea.toJson,
                data.intactForest2016Area.toJson,
                data.totalArea.toJson,
                data.protectedAreasArea.toJson,
                data.peatlandsArea.toJson,
                data.braBiomesArea.toJson,
                data.idnForestAreaArea.toJson,
                data.seAsiaLandCoverArea.toJson,
                data.idnLandCoverArea.toJson,
                data.idnForestMoratoriumArea.toJson,
                data.southAmericaPresence.toJson,
                data.legalAmazonPresence.toJson,
                data.braBiomesPresence.toJson,
                data.cerradoBiomesPresence.toJson,
                data.seAsiaPresence.toJson,
                data.idnPresence.toJson,
                data.forestValueIndicator.toJson,
                data.peatValueIndicator.toJson,
                data.protectedAreaValueIndicator.toJson,
                data.deforestationThreatIndicator.toJson,
                data.peatThreatIndicator.toJson,
                data.protectedAreaThreatIndicator.toJson,
                data.fireThreatIndicator.toJson
              )
            case _ =>
              throw new IllegalArgumentException("Not a SimpleFeatureId")
          }
      }
      .toDF(
        "location_id",
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
        "commodity_value_forest_extent", //  forestValueIndicator
        "commodity_value_peat", // peatValueIndicator
        "commodity_value_protected_areas", // protectedAreaValueIndicator
        "commodity_threat_deforestation", // deforestationThreatIndicator
        "commodity_threat_peat", // peatThreatIndicator
        "commodity_threat_protected_areas", // protectedAreaThreatIndicator
        "commodity_threat_fires" // fireThreatIndicator
      )
  }
}
