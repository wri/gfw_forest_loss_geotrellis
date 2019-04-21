package org.globalforestwatch.layers

class Mining(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/mining/$grid.tif"
}
