package org.globalforestwatch.summarystats.forest_change_diagnostic

import io.circe.syntax._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{CombinedFeatureId, FeatureId, GadmFeatureId, GfwProFeatureId, GridId, WdpaFeatureId}
import org.globalforestwatch.util.CaseClassConstrutor.createCaseClassFromMap

case class ForestChangeDiagnosticDFFactory(
                                            featureType: String,
                                            dataRDD: RDD[(FeatureId, ForestChangeDiagnosticData)],
                                            spark: SparkSession,
                                            kwargs: Map[String, Any]
                                          ) {

  import spark.implicits._

  def getDataFrame: DataFrame = {
    featureType match {
      case "gfwpro" => getFeatureDataFrame
      case "grid" => getGridFeatureDataFrame
      case "wdpa" => getWdpaFeatureDataFrame
      case "gadm" => getGadmFeatureDataFrame
      case _ =>
        throw new IllegalArgumentException("Not a valid FeatureType")
    }
  }

  private def getFeatureDataFrame: DataFrame = {

    dataRDD
      .map {
        case (id, data) =>
          id match {
            case gfwproId: GfwProFeatureId =>
              createCaseClassFromMap[ForestChangeDiagnosticRowSimple](
                Map("id" -> gfwproId.locationId.asJson.noSpaces) ++
                  featureFieldMap(data)
              )

            case _ =>
              throw new IllegalArgumentException("Not a GfwProFeatureId")
          }
      }
      .toDF("location_id" :: featureFieldNames: _*)
  }

  private def getGadmFeatureDataFrame: DataFrame = {

    dataRDD
      .map {
        case (id, data) =>
          id match {
            case gadmId: GadmFeatureId =>
              createCaseClassFromMap[ForestChangeDiagnosticRowSimple](
                Map(
                  "listId" -> "GADM 3.6",
                  "locationId" -> gadmId.toString) ++
                  featureFieldMap(data)
              )

            case _ =>
              throw new IllegalArgumentException("Not a GadmFeatureId")
          }
      }
      .toDF("list_id" :: "location_id" :: featureFieldNames: _*)
  }

  private def getWdpaFeatureDataFrame: DataFrame = {

    dataRDD
      .map {
        case (id, data) =>
          id match {
            case wdpaId: WdpaFeatureId =>
              createCaseClassFromMap[ForestChangeDiagnosticRowSimple](
                Map(
                  "listId" -> "WDPA",
                  "locationId" -> wdpaId.toString) ++
                  featureFieldMap(data)
              )

            case _ =>
              throw new IllegalArgumentException("Not a WdpaFeatureId")
          }
      }
      .toDF("list_id" :: "location_id" :: featureFieldNames: _*)
  }

  private def getGridFeatureDataFrame: DataFrame = {

    dataRDD
      .map {
        case (id, data) =>
          id match {
            case CombinedFeatureId(gfwproId: GfwProFeatureId, gridId: GridId) =>
              createCaseClassFromMap[ForestChangeDiagnosticRowGrid](
                Map(
                  "list_id" -> gfwproId.listId.asJson.noSpaces,
                  "location_id" -> gfwproId.locationId.asJson.noSpaces,
                  "x" -> gfwproId.x.asJson.noSpaces,
                  "y" -> gfwproId.y.asJson.noSpaces,
                  "grid" -> gridId.gridId.asJson.noSpaces
                ) ++
                  featureFieldMap(data)
                  ++
                  gridFieldMap(data)
              )
            case _ =>
              throw new IllegalArgumentException("Not a CombinedFeatureId")
          }

      }
      .toDF("list_id" :: "location_id" :: "grid" :: featureFieldNames ++ gridFieldNames: _*)
  }

  private def featureFieldMap(data: ForestChangeDiagnosticData) = {
    Map(
      "treeCoverLossTcd30Yearly" -> data.treeCoverLossTcd30Yearly.toJson,
      "treeCoverLossPrimaryForestYearly" -> data.treeCoverLossPrimaryForestYearly.toJson,
      "treeCoverLossPeatLandYearly" -> data.treeCoverLossPeatLandYearly.toJson,
      "treeCoverLossIntactForestYearly" -> data.treeCoverLossIntactForestYearly.toJson,
      "treeCoverLossProtectedAreasYearly" -> data.treeCoverLossProtectedAreasYearly.toJson,
      "treeCoverLossSEAsiaLandCoverYearly" -> data.treeCoverLossSEAsiaLandCoverYearly.toJson,
      "treeCoverLossIDNLandCoverYearly" -> data.treeCoverLossIDNLandCoverYearly.toJson,
      "treeCoverLossSoyPlanedAreasYearly" -> data.treeCoverLossSoyPlanedAreasYearly.toJson,
      "treeCoverLossIDNForestAreaYearly" -> data.treeCoverLossIDNForestAreaYearly.toJson,
      "treeCoverLossIDNForestMoratoriumYearly" -> data.treeCoverLossIDNForestMoratoriumYearly.toJson,
      "prodesLossYearly" -> data.prodesLossYearly.toJson,
      "prodesLossProtectedAreasYearly" -> data.prodesLossProtectedAreasYearly.toJson,
      "prodesLossProdesPrimaryForestYearly" -> data.prodesLossProdesPrimaryForestYearly.toJson,
      "treeCoverLossBRABiomesYearly" -> data.treeCoverLossBRABiomesYearly.toJson,
      "treeCoverExtent" -> data.treeCoverExtent.toJson,
      "treeCoverExtentPrimaryForest" -> data.treeCoverExtentPrimaryForest.toJson,
      "treeCoverExtentProtectedAreas" -> data.treeCoverExtentProtectedAreas.toJson,
      "treeCoverExtentPeatlands" -> data.treeCoverExtentPeatlands.toJson,
      "treeCoverExtentIntactForests" -> data.treeCoverExtentIntactForests.toJson,
      "primaryForestArea" -> data.primaryForestArea.toJson,
      "intactForest2016Area" -> data.intactForest2016Area.toJson,
      "totalArea" -> data.totalArea.toJson,
      "protectedAreasArea" -> data.protectedAreasArea.toJson,
      "peatlandsArea" -> data.peatlandsArea.toJson,
      "braBiomesArea" -> data.braBiomesArea.toJson,
      "idnForestAreaArea" -> data.idnForestAreaArea.toJson,
      "seAsiaLandCoverArea" -> data.seAsiaLandCoverArea.toJson,
      "idnLandCoverArea" -> data.idnLandCoverArea.toJson,
      "idnForestMoratoriumArea" -> data.idnForestMoratoriumArea.toJson,
      "southAmericaPresence" -> data.southAmericaPresence.toJson,
      "legalAmazonPresence" -> data.legalAmazonPresence.toJson,
      "braBiomesPresence" -> data.braBiomesPresence.toJson,
      "cerradoBiomesPresence" -> data.cerradoBiomesPresence.toJson,
      "seAsiaPresence" -> data.seAsiaPresence.toJson,
      "idnPresence" -> data.idnPresence.toJson,
      "forestValueIndicator" -> data.forestValueIndicator.toJson,
      "peatValueIndicator" -> data.peatValueIndicator.toJson,
      "protectedAreaValueIndicator" -> data.protectedAreaValueIndicator.toJson,
      "deforestationThreatIndicator" -> data.deforestationThreatIndicator.toJson,
      "peatThreatIndicator" -> data.peatThreatIndicator.toJson,
      "protectedAreaThreatIndicator" -> data.protectedAreaThreatIndicator.toJson,
      "fireThreatIndicator" -> data.fireThreatIndicator.toJson
    )
  }

  private def gridFieldMap(data: ForestChangeDiagnosticData) = {
    Map(
      "treeCoverLossTcd90Yearly" -> data.treeCoverLossTcd90Yearly.toJson,
      "filteredTreeCoverExtent" -> data.filteredTreeCoverExtent.toJson,
      "filteredTreeCoverExtentYearly" -> data.filteredTreeCoverExtentYearly.toJson,
      "filteredTreeCoverLossYearly" -> data.filteredTreeCoverLossYearly.toJson,
      "filteredTreeCoverLossPeatYearly" -> data.filteredTreeCoverLossPeatYearly.toJson,
      "filteredTreeCoverLossProtectedAreasYearly" -> data.filteredTreeCoverLossProtectedAreasYearly.toJson,
      "plantationArea" -> data.plantationArea.toJson,
      "plantationOnPeatArea" -> data.plantationOnPeatArea.toJson,
      "plantationInProtectedAreasArea" -> data.plantationInProtectedAreasArea.toJson
    )
  }

  val featureFieldNames = List(
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

  val gridFieldNames = List(
    "tree_cover_Loss_tcd90_yearly", // treeCoverLossTcd90Yearly
    "filtered_tree_cover_extent", // filteredTreeCoverExtent
    "filtered_tree_cover_extent_yearly", //filteredTreeCoverExtentYearly
    "filtered_tree_cover_loss_yearly", //filteredTreeCoverLossYearly
    "filtered_tree_cover_loss_peat_yearly", //filteredTreeCoverLossPeatYearly
    "filtered_tree_cover_loss_protected_areas_yearly", // filteredTreeCoverLossProtectedAreasYearly
    "plantation_area", // plantationArea
    "plantation_on_peat_area", // plantationOnPeatArea
    "plantation_in_protected_areas_area" //plantationInProtectedAreasArea
  )

}
