package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{BurnedAreasFeatureId, GadmFeatureId}

case class BurnedAreasRowGadm(burnedAreasId: BurnedAreasFeatureId,
                              gadmId: GadmFeatureId,
                              dataGroup: FireAlertsDataGroup,
                              data: FireAlertsData)