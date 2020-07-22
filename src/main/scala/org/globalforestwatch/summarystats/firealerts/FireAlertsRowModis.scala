package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{FireAlertModisFeatureId, GadmFeatureId}

case class FireAlertsRowModis(fireId: FireAlertModisFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)
