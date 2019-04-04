package org.globalforestwatch.layers

class Landmark(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"s3://wri-users/tmaschler/prep_tiles/landmark/${grid}.tif"
}
