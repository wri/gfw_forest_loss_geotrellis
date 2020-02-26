package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{SimpleFeatureId, ModisFireAlertFeatureId}

case class FireAlertsRowModisSimple(fireId: ModisFireAlertFeatureId,
                                  simpleId: SimpleFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)
