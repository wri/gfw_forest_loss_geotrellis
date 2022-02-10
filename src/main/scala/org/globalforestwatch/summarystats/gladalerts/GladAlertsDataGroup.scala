package org.globalforestwatch.summarystats.gladalerts

import org.globalforestwatch.util.Mercantile

case class GladAlertsDataGroup(alertDate: String,
                               isConfirmed: Option[Boolean],
                               tile: Mercantile.Tile,
                               climateMask: Boolean,
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
