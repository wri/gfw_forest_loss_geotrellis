package org.globalforestwatch.summarystats.integrated_alerts

import org.globalforestwatch.features.GadmFeatureId

case class IntegratedAlertsRowGadm(id: GadmFeatureId,
                             dataGroup: IntegratedAlertsDataGroup,
                             data: IntegratedAlertsData)
