package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.GeostoreFeatureId

case class FireAlertsRowGeostore(id: GeostoreFeatureId,
                                 alertDate: String,
                                 dataGroup: FireAlertsDataGroup,
                                 data: FireAlertsData)
