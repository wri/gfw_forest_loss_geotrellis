package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.WdpaFeatureId

case class FireAlertsRowWdpa(id: WdpaFeatureId,
                             alertDate: String,
                             dataGroup: FireAlertsDataGroup,
                             data: FireAlertsData)
