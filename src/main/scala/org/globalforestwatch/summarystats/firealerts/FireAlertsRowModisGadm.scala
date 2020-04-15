package org.globalforestwatch.summarystats.firealerts
import org.globalforestwatch.features.{GadmFeatureId, FireAlertModisFeatureId}

case class FireAlertsRowModisGadm(fireId: FireAlertModisFeatureId,
                                  gadmId: GadmFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)
