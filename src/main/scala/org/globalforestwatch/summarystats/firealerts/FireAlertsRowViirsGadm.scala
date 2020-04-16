package org.globalforestwatch.summarystats.firealerts
import org.globalforestwatch.features.{GadmFeatureId, FireAlertViirsFeatureId}

case class FireAlertsRowViirsGadm(fireId: FireAlertViirsFeatureId,
                                  gadmId: GadmFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)
