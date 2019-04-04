package org.globalforestwatch.layers

class Mangroves2016(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/mangroves_2016/${grid}.tif"
}
