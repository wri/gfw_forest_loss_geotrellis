package org.globalforestwatch.summarystats.integrated_alerts

import java.time.LocalDate

import org.globalforestwatch.util.Mercantile

case class IntegratedAlertsDataGroup(gladLAlertDate: Option[String],
                                     gladS2AlertDate: Option[String],
                                     raddAlertDate: Option[String],
                                     gladLConfidence: String,
                                     gladS2Confidence: String,
                                     raddConfidence: String,
                                     integratedAlertDate: Option[String],
                                     integratedConfidence: String,
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
                                     mangroves2016: Boolean,
                                     intactForestLandscapes2016: Boolean,
                                     braBiomes: String)
