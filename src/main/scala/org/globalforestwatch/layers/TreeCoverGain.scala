package org.globalforestwatch.layers

class TreeCoverGain(grid: String) extends IntegerLayer with RequiredILayer {
  val uri: String = s"$basePath/gain/$grid.tif"

  override def lookup(value: Int): Integer = if (value == 0) 0 else 1
}
