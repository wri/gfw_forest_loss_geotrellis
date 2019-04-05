package org.globalforestwatch.layers

class Mangroves1996(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/mangroves_1996/$grid.tif"
}
