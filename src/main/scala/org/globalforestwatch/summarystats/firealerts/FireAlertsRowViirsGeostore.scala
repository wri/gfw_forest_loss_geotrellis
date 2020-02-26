package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{GeostoreFeatureId, ViirsFireAlertFeatureId}

case class FireAlertsRowViirsGeostore(fireId: ViirsFireAlertFeatureId,
                                      geostoreId: GeostoreFeatureId,
                                      dataGroup: FireAlertsDataGroup,
                                      data: FireAlertsData)
