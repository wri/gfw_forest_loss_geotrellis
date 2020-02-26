package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{GeostoreFeatureId, ModisFireAlertFeatureId}

case class FireAlertsRowModisGeostore(fireId: ModisFireAlertFeatureId,
                                      geostoreId: GeostoreFeatureId,
                                      dataGroup: FireAlertsDataGroup,
                                      data: FireAlertsData)
