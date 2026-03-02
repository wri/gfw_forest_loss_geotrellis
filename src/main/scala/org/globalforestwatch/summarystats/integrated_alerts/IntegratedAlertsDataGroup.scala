package org.globalforestwatch.summarystats.integrated_alerts


case class IntegratedAlertsDataGroup(//gladLAlertDate: Option[String],
                                     gladS2AlertDate: Option[String],
                                     raddAlertDate: Option[String],
                                     //gladLConfidence: String,
                                     gladS2Confidence: Option[String],
                                     raddConfidence: Option[String],
                                     intDistAlertDate: Option[String],
                                     intDistConfidence: Option[String],
                                     primaryForest: Boolean,
                                     protectedAreas: String,
                                     landmark: Boolean,
                                     peatlands: Boolean,
                                     mangroves2020: Boolean,
                                     intactForestLandscapes2016: Boolean,
                                     naturalForests: String,
                                     treeCover2022: Boolean)
