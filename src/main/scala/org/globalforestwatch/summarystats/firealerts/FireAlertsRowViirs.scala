package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{FireAlertViirsFeatureId, GadmFeatureId}

case class FireAlertsRowViirs(fireId: FireAlertViirsFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)
