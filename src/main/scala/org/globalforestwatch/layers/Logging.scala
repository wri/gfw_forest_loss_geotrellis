package org.globalforestwatch.layers

class Logging(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/logging/$grid.tif"
}
