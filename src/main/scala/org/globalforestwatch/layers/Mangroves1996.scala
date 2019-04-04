package org.globalforestwatch.layers

class Mangroves1996(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/mangroves_1996/${grid}.tif"
}
