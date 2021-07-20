package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{BurnedAreasFeatureId, GeostoreFeatureId}

case class BurnedAreasRowGeostore(fireId: BurnedAreasFeatureId,
                                  geostoreId: GeostoreFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)