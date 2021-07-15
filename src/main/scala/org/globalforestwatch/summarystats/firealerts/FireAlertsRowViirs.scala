package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{FireAlertViirsFeatureId}

case class FireAlertsRowViirs(fireId: FireAlertViirsFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)
