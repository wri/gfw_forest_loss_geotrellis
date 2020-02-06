package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.SimpleFeatureId

case class FireAlertsRowSimple(id: SimpleFeatureId,
                               alertDate: String,
                               dataGroup: FireAlertsDataGroup,
                               data: FireAlertsData)
