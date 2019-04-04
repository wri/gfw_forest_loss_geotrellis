package org.globalforestwatch.layers

class MexicoPaymentForEcosystemServices(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String = s"s3://wri-users/tmaschler/prep_tiles/mex_psa/${grid}.tif"
}
