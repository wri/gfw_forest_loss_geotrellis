package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{SimpleFeatureId, FireAlertViirsFeatureId}

case class FireAlertsRowViirsSimple(fireId: FireAlertViirsFeatureId,
                                    simpleId: SimpleFeatureId,
                                    dataGroup: FireAlertsDataGroup,
                                    data: FireAlertsData)
