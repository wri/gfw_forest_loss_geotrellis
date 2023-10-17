package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{FireAlertModisFeatureId}

case class FireAlertsRowModis(fireId: FireAlertModisFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)
