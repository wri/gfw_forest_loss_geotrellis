package org.globalforestwatch.summarystats.firealerts
import org.globalforestwatch.features.{GadmFeatureId, ModisFireAlertFeatureId}

case class FireAlertsRowModisGadm(fireId: ModisFireAlertFeatureId,
                                  gadmId: GadmFeatureId,
                                  dataGroup: FireAlertsDataGroup,
                                  data: FireAlertsData)
