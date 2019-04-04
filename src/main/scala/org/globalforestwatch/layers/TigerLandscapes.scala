package org.globalforestwatch.layers

class TigerLandscapes(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/tiger_landscapes/${grid}.tif"
}
