package org.globalforestwatch.layers

class MexicoPaymentForEcosystemServices(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String = s"$basePath/mex_psa/$grid.tif"
}
