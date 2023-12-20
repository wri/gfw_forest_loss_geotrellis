package org.globalforestwatch.summarystats.gladalerts

import org.globalforestwatch.util.Mercantile

case class GladAlertsDataGroup(alertDate: String,
                               isConfirmed: Boolean,
                               tile: Mercantile.Tile,
                               climateMask: Boolean,
                               primaryForest: Boolean,
                               protectedAreas: String,
                               aze: Boolean,
                               keyBiodiversityAreas: Boolean,
                               landmark: Boolean,
                               plantedForests: String,
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
                               mangroves2020: Boolean,
                               intactForestLandscapes2016: Boolean,
                               braBiomes: String)
