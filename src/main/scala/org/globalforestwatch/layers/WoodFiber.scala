package org.globalforestwatch.layers

class WoodFiber(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/wood_fiber/${grid}.tif"
}
