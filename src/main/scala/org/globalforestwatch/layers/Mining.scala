package org.globalforestwatch.layers

case class Mining(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/mining/$grid.tif"
}
