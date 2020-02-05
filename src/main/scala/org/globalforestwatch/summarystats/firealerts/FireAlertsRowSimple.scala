package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.SimpleFeatureId

case class FireAlertsRowSimple(id: SimpleFeatureId,
                               dataGroup: FireAlertsDataGroup,
                               data: FireAlertsData)
