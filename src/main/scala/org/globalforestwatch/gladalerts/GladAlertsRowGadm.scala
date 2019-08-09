package org.globalforestwatch.gladalerts

import org.globalforestwatch.features.GadmFeatureId

case class GladAlertsRowGadm(id: GadmFeatureId,
                             dataGroup: GladAlertsDataGroup,
                             data: GladAlertsData)
