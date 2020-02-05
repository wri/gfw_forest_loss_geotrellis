package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.GadmFeatureId

case class FireAlertsRowGadm(id: GadmFeatureId,
                             alertDate: String,
                             dataGroup: FireAlertsDataGroup,
                             data: FireAlertsData)
