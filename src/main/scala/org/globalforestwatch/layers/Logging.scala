package org.globalforestwatch.layers

case class Logging(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/logging/$grid.tif"
}
