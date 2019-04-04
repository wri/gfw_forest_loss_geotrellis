package org.globalforestwatch.layers

class IntactForestLandscapes(grid: String)
    extends IntegerLayer
    with OptionalILayer {
  val uri: String = s"s3://wri-users/tmaschler/prep_tiles/ifl/${grid}.tif"
}
