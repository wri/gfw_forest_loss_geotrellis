package org.globalforestwatch.layers

class BiomassPerHectar(grid: String) extends DoubleLayer with RequiredDLayer {
  val uri: String = s"$basePath/biomass/$grid.tif"
}
