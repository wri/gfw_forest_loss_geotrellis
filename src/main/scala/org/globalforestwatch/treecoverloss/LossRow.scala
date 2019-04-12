package org.globalforestwatch.treecoverloss

case class LossRow(country: String,
                   admin1: String,
                   admin2: String,
                   loss: Integer,
                   tcd2000: Integer,
                   tcd2010: Integer,
                   drivers: String,
                   globalLandCover: String,
                   primaryForest: Boolean,
                   idnPrimaryForest: Boolean,
                   erosion: String,
                   // biodiversitySignificance: Boolean,
                   // biodiversityIntactness: Boolean,
                   wdpa: String,
                   plantations: String,
                   riverBasins: String,
                   ecozones: String,
                   urbanWatersheds: Boolean,
                   mangroves1996: Boolean,
                   mangroves2016: Boolean,
                   waterStress: String,
                   intactForestLandscapes: Integer,
                   endemicBirdAreas: Boolean,
                   tigerLandscapes: Boolean,
                   landmark: Boolean,
                   landRights: Boolean,
                   keyBiodiversityAreas: Boolean,
                   mining: Boolean,
                   rspo: String,
                   peatlands: Boolean,
                   oilPalm: Boolean,
                   idnForestMoratorium: Boolean,
                   idnLandCover: String,
                   mexProtectedAreas: Boolean,
                   mexPaymentForEcosystemServices: Boolean,
                   mexForestZoning: String,
                   perProductionForest: Boolean,
                   perProtectedAreas: Boolean,
                   perForestConcessions: String,
                   braBiomes: String,
                   woodFiber: Boolean,
                   resourceRights: Boolean,
                   logging: Boolean,
                   oilGas: Boolean,
                   totalArea: Double,
                   totalGainArea: Double,
                   totalBiomass: Double,
                   totalCo2: Double,
                   meanBiomass: Option[Double],
                   totalMangroveBiomass: Double,
                   totalMangroveCo2: Double,
                   meanMangroveBiomass: Option[Double])
