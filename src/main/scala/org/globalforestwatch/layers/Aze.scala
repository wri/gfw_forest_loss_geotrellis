package org.globalforestwatch.layers

class Aze(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/aze/$grid.tif"
}
