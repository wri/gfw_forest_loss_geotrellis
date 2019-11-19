package org.globalforestwatch.summarystats.gladalerts

import org.globalforestwatch.features.{GeostoreFeatureId, SimpleFeatureId}

case class GladAlertsRowGeostore(id: GeostoreFeatureId,
                               dataGroup: GladAlertsDataGroup,
                               data: GladAlertsData)
