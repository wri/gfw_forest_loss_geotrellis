package org.globalforestwatch.layers

class TreeCoverLoss(grid: String) extends IntegerLayer with RequiredILayer {
  val uri: String = s"$basePath/loss/$grid.tif"

  override def lookup(value: Int): Integer = if (value == 0) null else value + 2000
}
