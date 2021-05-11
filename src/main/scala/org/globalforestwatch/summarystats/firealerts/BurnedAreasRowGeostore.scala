package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{BurnedAreasFeatureId, GeostoreFeatureId}

case class BurnedAreasRowGeostore(burnedAreasId: BurnedAreasFeatureId,
                                  geostoreId: GeostoreFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)