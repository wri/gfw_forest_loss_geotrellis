package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{FireAlertViirsFeatureId, WdpaFeatureId}

case class FireAlertsRowViirsWdpa(fireId: FireAlertViirsFeatureId,
                                  wdpaId: WdpaFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)
