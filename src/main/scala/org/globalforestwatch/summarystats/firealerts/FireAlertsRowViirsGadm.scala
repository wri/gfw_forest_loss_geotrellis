package org.globalforestwatch.summarystats.firealerts
import org.globalforestwatch.features.{GadmFeatureId, ViirsFireAlertFeatureId}

case class FireAlertsRowViirsGadm(fireId: ViirsFireAlertFeatureId,
                                  gadmId: GadmFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)
