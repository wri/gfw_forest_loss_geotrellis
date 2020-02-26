package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{ViirsFireAlertFeatureId, WdpaFeatureId}

case class FireAlertsRowViirsWdpa(fireId: ViirsFireAlertFeatureId,
                                  wdpaId: WdpaFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)
