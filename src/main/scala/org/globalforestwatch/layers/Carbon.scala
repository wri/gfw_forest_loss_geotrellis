package org.globalforestwatch.layers

class Carbon(grid: String) extends DoubleLayer with RequiredDLayer {
  val uri: String = s"s3://wri-users/tmaschler/prep_tiles/co2_pixel/${grid}.tif"
}
