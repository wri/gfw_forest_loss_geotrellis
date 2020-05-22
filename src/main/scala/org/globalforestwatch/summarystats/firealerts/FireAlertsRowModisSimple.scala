package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{SimpleFeatureId, FireAlertModisFeatureId}

case class FireAlertsRowModisSimple(fireId: FireAlertModisFeatureId,
                                    simpleId: SimpleFeatureId,
                                    dataGroup: FireAlertsDataGroup,
                                    data: FireAlertsData)
