package org.globalforestwatch.summarystats.forest_change_diagnostic

import io.circe.syntax._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.features.{FeatureId, SimpleFeatureId}

import scala.collection.immutable.SortedMap

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
      .flatMap {
        case (id, summary) =>
          // We need to convert the Map to a List in order to correctly flatmap the data
          summary.stats.toList.map {
            case (dataGroup, data) =>
              (
                id,
                ForestChangeDiagnosticData(
                  treeCoverLossTcd30Yearly =
                    ForestChangeDiagnosticDataLossYearly.fill(
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      dataGroup.isUMDLoss
                    ),
                  treeCoverLossTcd90Yearly =
                    ForestChangeDiagnosticDataLossYearly.fill(
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      dataGroup.isUMDLoss && dataGroup.isTreeCoverExtent90
                    ),
                  treeCoverLossPrimaryForestYearly =
                    ForestChangeDiagnosticDataLossYearly.fill(
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      dataGroup.isPrimaryForest && dataGroup.isUMDLoss
                    ),
                  treeCoverLossPeatLandYearly =
                    ForestChangeDiagnosticDataLossYearly.fill(
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      dataGroup.isPeatlands && dataGroup.isUMDLoss
                    ),
                  treeCoverLossIntactForestYearly =
                    ForestChangeDiagnosticDataLossYearly.fill(
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      dataGroup.isIntactForestLandscapes2016 && dataGroup.isUMDLoss
                    ),
                  treeCoverLossProtectedAreasYearly =
                    ForestChangeDiagnosticDataLossYearly.fill(
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      dataGroup.isProtectedArea && dataGroup.isUMDLoss
                    ),
                  treeCoverLossSEAsiaLandCoverYearly =
                    ForestChangeDiagnosticDataLossYearlyCategory.fill(
                      dataGroup.seAsiaLandCover,
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      include = dataGroup.isUMDLoss
                    ),
                  treeCoverLossIDNLandCoverYearly =
                    ForestChangeDiagnosticDataLossYearlyCategory.fill(
                      dataGroup.idnLandCover,
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      include = dataGroup.isUMDLoss
                    ),
                  treeCoverLossSoyPlanedAreasYearly =
                    ForestChangeDiagnosticDataLossYearly.fill(
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      dataGroup.isSoyPlantedAreas && dataGroup.isUMDLoss
                    ),
                  treeCoverLossIDNForestAreaYearly =
                    ForestChangeDiagnosticDataLossYearlyCategory.fill(
                      dataGroup.idnForestArea,
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      include = dataGroup.isUMDLoss
                    ),
                  treeCoverLossIDNForestMoratoriumYearly =
                    ForestChangeDiagnosticDataLossYearly.fill(
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      dataGroup.isIdnForestMoratorium && dataGroup.isUMDLoss
                    ),
                  prodesLossYearly = ForestChangeDiagnosticDataLossYearly.fill(
                    dataGroup.prodesLossYear,
                    data.totalArea,
                    dataGroup.isProdesLoss
                  ),
                  prodesLossProtectedAreasYearly =
                    ForestChangeDiagnosticDataLossYearly.fill(
                      dataGroup.prodesLossYear,
                      data.totalArea,
                      dataGroup.isProdesLoss && dataGroup.isProtectedArea
                    ),
                  prodesLossProdesPrimaryForestYearly =
                    ForestChangeDiagnosticDataLossYearly.fill(
                      dataGroup.prodesLossYear,
                      data.totalArea,
                      dataGroup.isProdesLoss && dataGroup.isPrimaryForest
                    ),
                  treeCoverLossBRABiomesYearly =
                    ForestChangeDiagnosticDataLossYearlyCategory.fill(
                      dataGroup.braBiomes,
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      include = dataGroup.isUMDLoss
                    ),
                  treeCoverExtent = ForestChangeDiagnosticDataDouble
                    .fill(data.totalArea, dataGroup.isTreeCoverExtent30),
                  treeCoverExtentPrimaryForest =
                    ForestChangeDiagnosticDataDouble.fill(
                      data.totalArea,
                      dataGroup.isTreeCoverExtent30 && dataGroup.isPrimaryForest
                    ),
                  treeCoverExtentProtectedAreas =
                    ForestChangeDiagnosticDataDouble.fill(
                      data.totalArea,
                      dataGroup.isTreeCoverExtent30 && dataGroup.isProtectedArea
                    ),
                  treeCoverExtentPeatlands =
                    ForestChangeDiagnosticDataDouble.fill(
                      data.totalArea,
                      dataGroup.isTreeCoverExtent30 && dataGroup.isPeatlands
                    ),
                  treeCoverExtentIntactForests =
                    ForestChangeDiagnosticDataDouble.fill(
                      data.totalArea,
                      dataGroup.isTreeCoverExtent30 && dataGroup.isIntactForestLandscapes2016
                    ),
                  primaryForestArea = ForestChangeDiagnosticDataDouble
                    .fill(data.totalArea, dataGroup.isPrimaryForest),
                  intactForest2016Area = ForestChangeDiagnosticDataDouble.fill(
                    data.totalArea,
                    dataGroup.isIntactForestLandscapes2016
                  ),
                  totalArea =
                    ForestChangeDiagnosticDataDouble.fill(data.totalArea),
                  protectedAreasArea = ForestChangeDiagnosticDataDouble
                    .fill(data.totalArea, dataGroup.isProtectedArea),
                  peatlandsArea = ForestChangeDiagnosticDataDouble
                    .fill(data.totalArea, dataGroup.isPeatlands),
                  braBiomesArea = ForestChangeDiagnosticDataDoubleCategory
                    .fill(dataGroup.braBiomes, data.totalArea),
                  idnForestAreaArea = ForestChangeDiagnosticDataDoubleCategory
                    .fill(dataGroup.idnForestArea, data.totalArea),
                  seAsiaLandCoverArea = ForestChangeDiagnosticDataDoubleCategory
                    .fill(dataGroup.seAsiaLandCover, data.totalArea),
                  idnLandCoverArea = ForestChangeDiagnosticDataDoubleCategory
                    .fill(dataGroup.idnLandCover, data.totalArea),
                  idnForestMoratoriumArea = ForestChangeDiagnosticDataDouble
                    .fill(data.totalArea, dataGroup.isIdnForestMoratorium),
                  southAmericaPresence = ForestChangeDiagnosticDataBoolean
                    .fill(dataGroup.southAmericaPresence),
                  legalAmazonPresence = ForestChangeDiagnosticDataBoolean
                    .fill(dataGroup.legalAmazonPresence),
                  braBiomesPresence = ForestChangeDiagnosticDataBoolean
                    .fill(dataGroup.braBiomesPresence),
                  cerradoBiomesPresence = ForestChangeDiagnosticDataBoolean
                    .fill(dataGroup.cerradoBiomesPresence),
                  seAsiaPresence = ForestChangeDiagnosticDataBoolean.fill(
                    dataGroup.seAsiaPresence
                  ),
                  idnPresence = ForestChangeDiagnosticDataBoolean.fill(
                    dataGroup.idnPresence
                  ),
                  filteredTreeCoverExtentYearly =
                    ForestChangeDiagnosticDataExtentYearly.fill(
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      dataGroup.isTreeCoverExtent90 && !dataGroup.isPlantation
                    ),
                  filteredTreeCoverLossYearly =
                    ForestChangeDiagnosticDataLossYearly.fill(
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      dataGroup.isUMDLoss && dataGroup.isTreeCoverExtent90 && !dataGroup.isPlantation
                    ),
                  filteredTreeCoverLossPeatYearly =
                    ForestChangeDiagnosticDataLossYearly.fill(
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      dataGroup.isUMDLoss && dataGroup.isTreeCoverExtent90 && !dataGroup.isPlantation && dataGroup.isPeatlands
                    ),
                  filteredTreeCoverLossProtectedAreasYearly =
                    ForestChangeDiagnosticDataLossYearly.fill(
                      dataGroup.umdTreeCoverLossYear,
                      data.totalArea,
                      dataGroup.isUMDLoss && dataGroup.isTreeCoverExtent90 && !dataGroup.isPlantation && dataGroup.isProtectedArea
                    ),
                  plantationArea = ForestChangeDiagnosticDataDouble
                    .fill(data.totalArea, dataGroup.isPlantation),
                  plantationOnPeatArea = ForestChangeDiagnosticDataDouble
                    .fill(data.totalArea, dataGroup.isPlantation && dataGroup.isPeatlands),
                  plantationInProtectedAreasArea = ForestChangeDiagnosticDataDouble
                    .fill(data.totalArea, dataGroup.isPlantation && dataGroup.isProtectedArea),
                  forestValueIndicator =
                    ForestChangeDiagnosticDataLossYearly.empty,
                  peatValueIndicator = ForestChangeDiagnosticDataDouble.empty,
                  protectedAreaValueIndicator =
                    ForestChangeDiagnosticDataDouble.empty,
                  deforestationThreatIndicator =
                    ForestChangeDiagnosticDataLossYearly.empty,
                  peatThreatIndicator =
                    ForestChangeDiagnosticDataLossYearly.empty,
                  protectedAreaThreatIndicator =
                    ForestChangeDiagnosticDataLossYearly.empty,
                  fireThreatIndicator =
                    ForestChangeDiagnosticDataLossYearly.empty
                )
              )

          }
      }
      .reduceByKey(_ merge _)
      .map {
        case (id, data) =>
          val minLossYear = data.treeCoverLossTcd90Yearly.value.keysIterator.min
          val maxLossYear = data.treeCoverLossTcd90Yearly.value.keysIterator.max
          val years: List[Int] = List.range(minLossYear, maxLossYear + 1)

          val forestValueIndicator: SortedMap[Int, Double] =
            data.filteredTreeCoverExtentYearly.value
          val peatValueIndicator: Double = data.peatlandsArea.value
          val protectedAreaValueIndicator: Double =
            data.protectedAreasArea.value
          val deforestationThreatIndicator: SortedMap[Int, Double] = SortedMap(
            years.map(
              year =>
                (year, {

                  // Somehow the compiler cannot infer the types correctly
                  // I hence declare them here explicitly to help him out.
                  val thisYearLoss: Double = data.filteredTreeCoverLossYearly.value
                    .getOrElse(year, 0)

                  val lastYearLoss: Double = data.filteredTreeCoverLossYearly.value
                    .getOrElse(year - 1, 0)

                  thisYearLoss + lastYearLoss
                })
            ): _*
          )

          val peatThreatIndicator: SortedMap[Int, Double] = SortedMap(
            years.map(
              year =>
                (
                  year, {
                  // Somehow the compiler cannot infer the types correctly
                  // I hence declare them here explicitly to help him out.
                  val thisYearPeatLoss: Double = data.filteredTreeCoverLossPeatYearly.value
                    .getOrElse(year, 0)

                  val lastYearPeatLoss: Double = data.filteredTreeCoverLossPeatYearly.value
                    .getOrElse(year - 1, 0)

                  thisYearPeatLoss + lastYearPeatLoss + data.plantationOnPeatArea.value


                }
                )
            ): _*
          )
          val protectedAreaThreatIndicator: SortedMap[Int, Double] = SortedMap(
            years.map(
              year =>
                (
                  year, {
                  // Somehow the compiler cannot infer the types correctly
                  // I hence declare them here explicitly to help him out.
                  val thisYearProtectedAreaLoss: Double = data.filteredTreeCoverLossProtectedAreasYearly.value
                    .getOrElse(year, 0)

                  val lastYearProtectedAreaLoss: Double = data.filteredTreeCoverLossProtectedAreasYearly.value
                    .getOrElse(year - 1, 0)

                  thisYearProtectedAreaLoss + lastYearProtectedAreaLoss + data.plantationInProtectedAreasArea.value
                }
                )
            ): _*
          )
          //          val fireThreatIndicator: SortedMap[Int, Double] = ???

          val new_data = data.update(
            forestValueIndicator =
              ForestChangeDiagnosticDataLossYearly(forestValueIndicator),
            peatValueIndicator =
              ForestChangeDiagnosticDataDouble(peatValueIndicator),
            protectedAreaValueIndicator =
              ForestChangeDiagnosticDataDouble(protectedAreaValueIndicator),
            deforestationThreatIndicator = ForestChangeDiagnosticDataLossYearly(
              deforestationThreatIndicator
            ),
            peatThreatIndicator =
              ForestChangeDiagnosticDataLossYearly(peatThreatIndicator),
            protectedAreaThreatIndicator =
              ForestChangeDiagnosticDataLossYearly(protectedAreaThreatIndicator)
            //            fireThreatIndicator =
            //              ForestChangeDiagnosticDataLossYearly(fireThreatIndicator)
          )
          (id, new_data)
      }
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
                data.filteredTreeCoverExtentYearly.toJson,
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
        "filtered_tree_cover_yearly", // filteredTreeCoverExtentYearly
        "forest_value_indicator", //  forestValueIndicator
        "peat_value_indicator", // peatValueIndicator
        "protected_area_value_indicator", // protectedAreaValueIndicator
        "deforestation_threat_indicator", // deforestationThreatIndicator
        "peat_threat_indicator", // peatThreatIndicator
        "protected_area_threat_indicator", // protectedAreaThreatIndicator
        "fire_threat_indicator" // fireThreatIndicator
      )
  }
}
