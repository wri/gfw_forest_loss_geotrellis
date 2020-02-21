package org.globalforestwatch.layers

case class MekongTreeCoverLossExtent(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/Mekong_loss_extent/$grid.tif"
}

