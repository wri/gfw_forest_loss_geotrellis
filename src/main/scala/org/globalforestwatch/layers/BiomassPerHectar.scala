package org.globalforestwatch.layers

class BiomassPerHectar(grid: String) extends DoubleLayer with RequiredDLayer {
  val uri: String = s"s3://wri-users/tmaschler/prep_tiles/biomass/${grid}.tif"
}
