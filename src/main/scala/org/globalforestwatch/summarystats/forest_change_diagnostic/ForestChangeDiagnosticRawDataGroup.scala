package org.globalforestwatch.summarystats.forest_change_diagnostic

case class ForestChangeDiagnosticRawDataGroup(umdTreeCoverLossYear: Int,
                                              isUMDLoss: Boolean,
                                              prodesLossYear: Int,
                                              isProdesLoss: Boolean,
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
                                              southAmericaPresence: Boolean,
                                              legalAmazonPresence: Boolean,
                                              braBiomesPresence: Boolean,
                                              cerradoBiomesPresence: Boolean,
                                              seAsiaPresence: Boolean,
                                              idnPresence: Boolean)


