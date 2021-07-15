package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{BurnedAreasFeatureId, GadmFeatureId}

case class BurnedAreasRowGadm(fireId: BurnedAreasFeatureId,
                              gadmId: GadmFeatureId,
                              dataGroup: FireAlertsDataGroup,
                              data: FireAlertsData)