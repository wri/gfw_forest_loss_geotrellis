package org.globalforestwatch.layers

case class TreeCoverLossLegalAmazon(grid: String) extends IntegerLayer with OptionalILayer {
  val uri: String = s"$basePath/legal_Amazon_annual_loss/$grid.tif"

  override def lookup(value: Int): Integer = if (value == 0) null else value + 2000
}
