package org.globalforestwatch.summarystats.carbonflux

case class CarbonFluxDataGroup(threshold: Integer,
                               gain: Integer,
                               mangroveBiomassExtent: Boolean,
                               drivers: String,
                               ecozones: String,
                               landRights: Boolean,
                               wdpa: String,
                               intactForestLandscapes: String,
                               plantations: String,
                               primaryForest: Boolean)
