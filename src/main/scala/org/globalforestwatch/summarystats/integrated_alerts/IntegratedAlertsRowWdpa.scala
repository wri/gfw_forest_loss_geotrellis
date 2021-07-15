package org.globalforestwatch.summarystats.integrated_alerts

import org.globalforestwatch.features.WdpaFeatureId

case class IntegratedAlertsRowWdpa(id: WdpaFeatureId,
                             dataGroup: IntegratedAlertsDataGroup,
                             data: IntegratedAlertsData)
