package org.globalforestwatch.summarystats.carbonflux

case class CarbonFluxDataGroup(lossYear: Integer,
                               threshold: Integer,
                               gain: Integer,
                               mangroveBiomassExtent: Boolean,
                               drivers: String,
                               ecozones: String,
                               landRights: Boolean,
                               wdpa: String,
                               intactForestLandscapes: String,
                               plantations: String,
                               intactPrimaryForest: Boolean,
                               peatlandsFlux: Boolean)
