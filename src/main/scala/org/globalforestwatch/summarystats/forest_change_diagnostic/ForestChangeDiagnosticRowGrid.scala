package org.globalforestwatch.summarystats.forest_change_diagnostic

case class ForestChangeDiagnosticRowGrid(
                                          id: String,
                                          grid: String,
                                          treeCoverLossTcd30Yearly: String,
                                          treeCoverLossPrimaryForestYearly: String,
                                          treeCoverLossPeatLandYearly: String,
                                          treeCoverLossIntactForestYearly: String,
                                          treeCoverLossProtectedAreasYearly: String,
                                          treeCoverLossSEAsiaLandCoverYearly: String,
                                          treeCoverLossIDNLandCoverYearly: String,
                                          treeCoverLossSoyPlanedAreasYearly: String,
                                          treeCoverLossIDNForestAreaYearly: String,
                                          treeCoverLossIDNForestMoratoriumYearly: String,
                                          prodesLossYearly: String,
                                          prodesLossProtectedAreasYearly: String,
                                          prodesLossProdesPrimaryForestYearly: String,
                                          treeCoverLossBRABiomesYearly: String,
                                          treeCoverExtent: String,
                                          treeCoverExtentPrimaryForest: String,
                                          treeCoverExtentProtectedAreas: String,
                                          treeCoverExtentPeatlands: String,
                                          treeCoverExtentIntactForests: String,
                                          primaryForestArea: String,
                                          intactForest2016Area: String,
                                          totalArea: String,
                                          protectedAreasArea: String,
                                          peatlandsArea: String,
                                          braBiomesArea: String,
                                          idnForestAreaArea: String,
                                          seAsiaLandCoverArea: String,
                                          idnLandCoverArea: String,
                                          idnForestMoratoriumArea: String,
                                          southAmericaPresence: String,
                                          legalAmazonPresence: String,
                                          braBiomesPresence: String,
                                          cerradoBiomesPresence: String,
                                          seAsiaPresence: String,
                                          idnPresence: String,
                                          forestValueIndicator: String,
                                          peatValueIndicator: String,
                                          protectedAreaValueIndicator: String,
                                          deforestationThreatIndicator: String,
                                          peatThreatIndicator: String,
                                          protectedAreaThreatIndicator: String,
                                          fireThreatIndicator: String,
                                          // extra columns
                                          treeCoverLossTcd90Yearly: String,
                                          filteredTreeCoverExtent: String,
                                          filteredTreeCoverExtentYearly: String,
                                          filteredTreeCoverLossYearly: String,
                                          filteredTreeCoverLossPeatYearly: String,
                                          filteredTreeCoverLossProtectedAreasYearly: String,
                                          plantationArea: String,
                                          plantationOnPeatArea: String,
                                          plantationInProtectedAreasArea: String,
                                        )
