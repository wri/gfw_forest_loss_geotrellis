package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{ModisFireAlertFeatureId, WdpaFeatureId}

case class FireAlertsRowModisWdpa(fireId: ModisFireAlertFeatureId,
                                  wdpaId: WdpaFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)
