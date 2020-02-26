package org.globalforestwatch.layers

case class Aze(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/aze/v20190816/$grid.tif"
}
