package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.features.{GeostoreFeatureId, FireAlertModisFeatureId}

case class FireAlertsRowModisGeostore(fireId: FireAlertModisFeatureId,
                                      geostoreId: GeostoreFeatureId,
                                      dataGroup: FireAlertsDataGroup,
                                      data: FireAlertsData)
