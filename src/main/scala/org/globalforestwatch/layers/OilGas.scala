package org.globalforestwatch.layers

case class OilGas(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/oil_gas/$grid.tif"
}
