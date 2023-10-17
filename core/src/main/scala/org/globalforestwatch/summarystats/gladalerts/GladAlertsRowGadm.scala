package org.globalforestwatch.summarystats.gladalerts

import org.globalforestwatch.features.GadmFeatureId

case class GladAlertsRowGadm(id: GadmFeatureId,
                             dataGroup: GladAlertsDataGroup,
                             data: GladAlertsData)
