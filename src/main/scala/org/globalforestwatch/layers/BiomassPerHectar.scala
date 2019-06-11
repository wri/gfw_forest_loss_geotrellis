package org.globalforestwatch.layers

case class BiomassPerHectar(grid: String) extends DoubleLayer with OptionalDLayer {
  val uri: String = s"$basePath/biomass/$grid.tif"
}
