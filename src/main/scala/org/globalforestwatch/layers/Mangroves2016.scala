package org.globalforestwatch.layers

class Mangroves2016(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/mangroves_2016/$grid.tif"
}
