package org.globalforestwatch.summarystats.firealerts


case class FireAlertsDataGroup(threshold: Integer,
                               primaryForest: Boolean,
                               protectedAreas: String,
                               aze: Boolean,
                               keyBiodiversityAreas: Boolean,
                               landmark: Boolean,
                               plantations: String,
                               mining: Boolean,
                               logging: Boolean,
                               rspo: String,
                               woodFiber: Boolean,
                               peatlands: Boolean,
                               indonesiaForestMoratorium: Boolean,
                               oilPalm: Boolean,
                               indonesiaForestArea: String,
                               peruForestConcessions: String,
                               oilGas: Boolean,
                               mangroves2016: Boolean,
                               intactForestLandscapes2016: Boolean,
                               braBiomes: String)
