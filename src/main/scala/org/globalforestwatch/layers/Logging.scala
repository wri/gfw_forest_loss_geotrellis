package org.globalforestwatch.layers

class Logging(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"s3://wri-users/tmaschler/prep_tiles/logging/${grid}.tif"
}
