package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{FireAlertModisFeatureId, WdpaFeatureId}

case class FireAlertsRowModisWdpa(fireId: FireAlertModisFeatureId,
                                  wdpaId: WdpaFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)
