package org.globalforestwatch.summarystats.integrated_alerts

import org.globalforestwatch.features.SimpleFeatureId

case class IntegratedAlertsRowSimple(id: SimpleFeatureId,
                               dataGroup: IntegratedAlertsDataGroup,
                               data: IntegratedAlertsData)
