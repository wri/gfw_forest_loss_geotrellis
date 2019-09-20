package org.globalforestwatch.layers

case class TreeCoverGain(grid: String) extends BooleanLayer with RequiredILayer {
  val uri: String = s"$basePath/gain/$grid.tif"

  override val internalNoDataValue: Int = 0
  override val externalNoDataValue: Boolean = false

  override def lookup(value: Int): Boolean = if (value == 0) false else true
}
