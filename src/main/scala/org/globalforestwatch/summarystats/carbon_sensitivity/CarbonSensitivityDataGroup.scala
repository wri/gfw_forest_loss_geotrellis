package org.globalforestwatch.summarystats.carbon_sensitivity

case class CarbonSensitivityDataGroup(lossYear: Integer,
                               threshold: Integer,
                               isGain: Boolean,
                               isLoss: Boolean,
                               mangroveBiomassExtent: Boolean,
                               drivers: String,
                               ecozones: String,
                               landRights: Boolean,
                               wdpa: String,
                               intactForestLandscapes: String,
                               plantations: String,
                               intactPrimaryForest: Boolean,
                               peatlandsFlux: Boolean,
                               forestAgeCategory: String)
