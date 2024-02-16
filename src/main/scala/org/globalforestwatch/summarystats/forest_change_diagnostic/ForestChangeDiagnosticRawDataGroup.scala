package org.globalforestwatch.summarystats.forest_change_diagnostic

import org.globalforestwatch.layers.ApproxYear

case class ForestChangeDiagnosticRawDataGroup(umdTreeCoverLossYear: Int,
                                              isUMDLoss: Boolean,
                                              prodesLossYear: Int,
                                              isProdesLoss: Boolean,
                                              countryCode: String,
                                              countrySpecificLossYear: ApproxYear,
                                              isCountrySpecificLoss: Boolean,
                                              isTreeCoverExtent30: Boolean,
                                              isTreeCoverExtent90: Boolean,
                                              isPrimaryForest: Boolean,
                                              isPeatlands: Boolean,
                                              isIntactForestLandscapes2000: Boolean,
                                              isProtectedArea: Boolean,
                                              seAsiaLandCover: String,
                                              idnLandCover: String,
                                              isSoyPlantedAreas: Boolean,
                                              idnForestArea: String,
                                              isIdnForestMoratorium: Boolean,
                                              braBiomes: String,
                                              isPlantation: Boolean,
                                              argOTBN: String,
                                              southAmericaPresence: Boolean,
                                              legalAmazonPresence: Boolean,
                                              braBiomesPresence: Boolean,
                                              cerradoBiomesPresence: Boolean,
                                              seAsiaPresence: Boolean,
                                              idnPresence: Boolean,
                                              argPresence: Boolean,
                                              protectedAreaByCategory: String,
                                              landmarkByCategory: String,
) {

  /** Produce a partial ForestChangeDiagnosticData only for the loss year in this data group */
  def toForestChangeDiagnosticData(totalArea: Double): ForestChangeDiagnosticData = {
    ForestChangeDiagnosticData(
    tree_cover_loss_total_yearly = ForestChangeDiagnosticDataLossYearly.fill(
      umdTreeCoverLossYear,
      totalArea,
      isUMDLoss
    ),
    tree_cover_loss_tcd90_yearly = ForestChangeDiagnosticDataLossYearly.fill(
      umdTreeCoverLossYear,
      totalArea,
      isUMDLoss && isTreeCoverExtent90
    ),
    tree_cover_loss_primary_forest_yearly =
      ForestChangeDiagnosticDataLossYearly.fill(
        umdTreeCoverLossYear,
        totalArea,
        isPrimaryForest && isUMDLoss
      ),
    tree_cover_loss_peat_yearly = ForestChangeDiagnosticDataLossYearly.fill(
      umdTreeCoverLossYear,
      totalArea,
      isPeatlands && isUMDLoss
    ),
    tree_cover_loss_intact_forest_yearly =
      ForestChangeDiagnosticDataLossYearly.fill(
        umdTreeCoverLossYear,
        totalArea,
        isIntactForestLandscapes2000 && isUMDLoss
      ),
    tree_cover_loss_protected_areas_yearly =
      ForestChangeDiagnosticDataLossYearly.fill(
        umdTreeCoverLossYear,
        totalArea,
        isProtectedArea && isUMDLoss
      ),
    tree_cover_loss_by_country_yearly =
      ForestChangeDiagnosticDataLossYearlyCategory.fill(
        countryCode,
        umdTreeCoverLossYear,
        totalArea,
        include = isProtectedArea && isUMDLoss
      ),
    tree_cover_loss_arg_otbn_yearly = 
      ForestChangeDiagnosticDataLossYearlyCategory.fill(
      argOTBN, 
      countrySpecificLossYear.year,
      totalArea,
      include = isCountrySpecificLoss
    ),
    tree_cover_loss_sea_landcover_yearly =
      ForestChangeDiagnosticDataLossYearlyCategory.fill(
        seAsiaLandCover,
        umdTreeCoverLossYear,
        totalArea,
        include = isUMDLoss
      ),
    tree_cover_loss_idn_landcover_yearly =
      ForestChangeDiagnosticDataLossYearlyCategory.fill(
        idnLandCover,
        umdTreeCoverLossYear,
        totalArea,
        include = isUMDLoss
      ),
    tree_cover_loss_soy_yearly =
      ForestChangeDiagnosticDataLossYearly.fill(
        umdTreeCoverLossYear,
        totalArea,
        isSoyPlantedAreas && isUMDLoss
      ),
    tree_cover_loss_idn_legal_yearly =
      ForestChangeDiagnosticDataLossYearlyCategory.fill(
        idnForestArea,
        umdTreeCoverLossYear,
        totalArea,
        include = isUMDLoss
      ),
    tree_cover_loss_idn_forest_moratorium_yearly =
      ForestChangeDiagnosticDataLossYearly.fill(
        umdTreeCoverLossYear,
        totalArea,
        isIdnForestMoratorium && isUMDLoss
      ),
    tree_cover_loss_prodes_yearly = ForestChangeDiagnosticDataLossYearly.fill(
      prodesLossYear,
      totalArea,
      isProdesLoss
    ),
    tree_cover_loss_prodes_wdpa_yearly =
      ForestChangeDiagnosticDataLossYearly.fill(
        prodesLossYear,
        totalArea,
        isProdesLoss && isProtectedArea
      ),
    tree_cover_loss_prodes_primary_forest_yearly =
      ForestChangeDiagnosticDataLossYearly.fill(
        prodesLossYear,
        totalArea,
        isProdesLoss && isPrimaryForest
      ),
    country_code = ForestChangeDiagnosticDataDoubleCategory
      .fill(countryCode, totalArea),
    tree_cover_loss_country_specific_yearly = ForestChangeDiagnosticDataLossApproxYearlyCategory.fill(
      countryCode,
      countrySpecificLossYear,
      totalArea,
      include = isCountrySpecificLoss
    ),
    tree_cover_loss_country_specific_wdpa_yearly = ForestChangeDiagnosticDataLossApproxYearlyCategory.fill(
      protectedAreaByCategory,
      countrySpecificLossYear,
      totalArea,
      include = isCountrySpecificLoss && isProtectedArea
    ),
    tree_cover_loss_country_specific_primary_forest_yearly = ForestChangeDiagnosticDataLossApproxYearly.fill(
      countrySpecificLossYear,
      totalArea,
      isCountrySpecificLoss && isPrimaryForest
    ),
    tree_cover_loss_country_specific_landmark_yearly = ForestChangeDiagnosticDataLossApproxYearly.fill(
      countrySpecificLossYear,
      totalArea,
      isCountrySpecificLoss && landmarkByCategory != ""
    ),
    tree_cover_loss_brazil_biomes_yearly =
      ForestChangeDiagnosticDataLossYearlyCategory.fill(
        braBiomes,
        umdTreeCoverLossYear,
        totalArea,
        include = isUMDLoss
      ),
    tree_cover_extent_total = ForestChangeDiagnosticDataDouble
      .fill(totalArea, isTreeCoverExtent30),
    tree_cover_extent_primary_forest = ForestChangeDiagnosticDataDouble.fill(
      totalArea,
      isTreeCoverExtent30 && isPrimaryForest
    ),
    tree_cover_extent_protected_areas = ForestChangeDiagnosticDataDouble.fill(
      totalArea,
      isTreeCoverExtent30 && isProtectedArea
    ),
    tree_cover_extent_peat = ForestChangeDiagnosticDataDouble.fill(
      totalArea,
      isTreeCoverExtent30 && isPeatlands
    ),
    tree_cover_extent_intact_forest = ForestChangeDiagnosticDataDouble.fill(
      totalArea,
      isTreeCoverExtent30 && isIntactForestLandscapes2000
    ),
    natural_habitat_primary = ForestChangeDiagnosticDataDouble
      .fill(totalArea, isPrimaryForest),
    natural_habitat_intact_forest = ForestChangeDiagnosticDataDouble
      .fill(totalArea, isIntactForestLandscapes2000),
    total_area = ForestChangeDiagnosticDataDouble.fill(totalArea),
    protected_areas_area = ForestChangeDiagnosticDataDouble
      .fill(totalArea, isProtectedArea),
    peat_area = ForestChangeDiagnosticDataDouble
      .fill(totalArea, isPeatlands),
    arg_otbn_area = ForestChangeDiagnosticDataDoubleCategory
      .fill(argOTBN, totalArea),
    protected_areas_by_category_area = ForestChangeDiagnosticDataDoubleCategory
      .fill(protectedAreaByCategory, totalArea),
    landmark_by_category_area = ForestChangeDiagnosticDataDoubleCategory.fill(landmarkByCategory, totalArea),
    brazil_biomes = ForestChangeDiagnosticDataDoubleCategory
      .fill(braBiomes, totalArea),
    idn_legal_area = ForestChangeDiagnosticDataDoubleCategory
      .fill(idnForestArea, totalArea),
    sea_landcover_area = ForestChangeDiagnosticDataDoubleCategory
      .fill(seAsiaLandCover, totalArea),
    idn_landcover_area = ForestChangeDiagnosticDataDoubleCategory
      .fill(idnLandCover, totalArea),
    idn_forest_moratorium_area = ForestChangeDiagnosticDataDouble
      .fill(totalArea, isIdnForestMoratorium),
    south_america_presence = ForestChangeDiagnosticDataBoolean
      .fill(southAmericaPresence),
    legal_amazon_presence = ForestChangeDiagnosticDataBoolean
      .fill(legalAmazonPresence),
    brazil_biomes_presence = ForestChangeDiagnosticDataBoolean
      .fill(braBiomesPresence),
    cerrado_biome_presence = ForestChangeDiagnosticDataBoolean
      .fill(cerradoBiomesPresence),
    southeast_asia_presence =
      ForestChangeDiagnosticDataBoolean.fill(seAsiaPresence),
    indonesia_presence =
      ForestChangeDiagnosticDataBoolean.fill(idnPresence),
    argentina_presence =
      ForestChangeDiagnosticDataBoolean.fill(argPresence),
    filtered_tree_cover_extent = ForestChangeDiagnosticDataDouble
      .fill(
        totalArea,
        isTreeCoverExtent90 && !isPlantation
      ),
    filtered_tree_cover_extent_yearly =
      ForestChangeDiagnosticDataValueYearly.empty,
    filtered_tree_cover_loss_yearly = ForestChangeDiagnosticDataLossYearly.fill(
      umdTreeCoverLossYear,
      totalArea,
      isUMDLoss && isTreeCoverExtent90 && !isPlantation
    ),
    filtered_tree_cover_loss_peat_yearly =
      ForestChangeDiagnosticDataLossYearly.fill(
        umdTreeCoverLossYear,
        totalArea,
        isUMDLoss && isTreeCoverExtent90 && !isPlantation && isPeatlands
      ),
    filtered_tree_cover_loss_protected_areas_yearly =
      ForestChangeDiagnosticDataLossYearly.fill(
        umdTreeCoverLossYear,
        totalArea,
        isUMDLoss && isTreeCoverExtent90 && !isPlantation && isProtectedArea
      ),
    plantation_area = ForestChangeDiagnosticDataDouble
      .fill(totalArea, isPlantation),
    plantation_on_peat_area = ForestChangeDiagnosticDataDouble
      .fill(
        totalArea,
        isPlantation && isPeatlands
      ),
    plantation_in_protected_areas_area = ForestChangeDiagnosticDataDouble
      .fill(
        totalArea,
        isPlantation && isProtectedArea
      ),
    commodity_value_forest_extent = ForestChangeDiagnosticDataValueYearly.empty,
    commodity_value_peat = ForestChangeDiagnosticDataValueYearly.empty,
    commodity_value_protected_areas = ForestChangeDiagnosticDataValueYearly.empty,
    commodity_threat_deforestation = ForestChangeDiagnosticDataLossYearly.empty,
    commodity_threat_peat = ForestChangeDiagnosticDataLossYearly.empty,
    commodity_threat_protected_areas = ForestChangeDiagnosticDataLossYearly.empty,
    commodity_threat_fires = ForestChangeDiagnosticDataLossYearly.empty
  )
  }
}
