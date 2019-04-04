package org.globalforestwatch.layers

class Mining(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"s3://wri-users/tmaschler/prep_tiles/mining/${grid}.tif"
}
