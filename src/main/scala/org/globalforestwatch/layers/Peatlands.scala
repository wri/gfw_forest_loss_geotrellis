package org.globalforestwatch.layers

class Peatlands(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"s3://wri-users/tmaschler/prep_tiles/peatlands/${grid}.tif"
}
