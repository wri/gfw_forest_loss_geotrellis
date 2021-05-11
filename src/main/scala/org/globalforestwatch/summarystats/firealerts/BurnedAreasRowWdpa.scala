package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{BurnedAreasFeatureId, WdpaFeatureId}

case class BurnedAreasRowWdpa(burnedAreasId: BurnedAreasFeatureId,
                              wdpaId: WdpaFeatureId,
                              dataGroup: FireAlertsDataGroup,
                              data: FireAlertsData)