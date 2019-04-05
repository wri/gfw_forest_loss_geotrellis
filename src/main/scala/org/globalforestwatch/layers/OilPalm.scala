package org.globalforestwatch.layers

class OilPalm(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/oil_palm/$grid.tif"
}
