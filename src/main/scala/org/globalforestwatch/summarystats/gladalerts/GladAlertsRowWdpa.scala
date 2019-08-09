package org.globalforestwatch.summarystats.gladalerts

import org.globalforestwatch.features.WdpaFeatureId

case class GladAlertsRowWdpa(id: WdpaFeatureId,
                             dataGroup: GladAlertsDataGroup,
                             data: GladAlertsData)
