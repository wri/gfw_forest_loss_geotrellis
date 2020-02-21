package org.globalforestwatch.layers

case class TreeCoverLossFirstYear20012015Mekong(grid: String) extends IntegerLayer with OptionalILayer {
  val uri: String = s"$basePath/Mekong_first_year_annual_loss_2001_2015/$grid.tif"

  override def lookup(value: Int): Integer = if (value == 0) null else value + 2000
}
