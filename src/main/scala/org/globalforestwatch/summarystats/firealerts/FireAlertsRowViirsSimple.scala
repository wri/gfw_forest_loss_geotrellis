package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{SimpleFeatureId, ViirsFireAlertFeatureId}

case class FireAlertsRowViirsSimple(fireId: ViirsFireAlertFeatureId,
                                    simpleId: SimpleFeatureId,
                                    dataGroup: FireAlertsDataGroup,
                                    data: FireAlertsData)
