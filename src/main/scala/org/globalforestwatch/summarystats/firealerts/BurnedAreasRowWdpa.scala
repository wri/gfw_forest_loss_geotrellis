package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{BurnedAreasFeatureId, WdpaFeatureId}

case class BurnedAreasRowWdpa(fireId: BurnedAreasFeatureId,
                              wdpaId: WdpaFeatureId,
                              dataGroup: FireAlertsDataGroup,
                              data: FireAlertsData)