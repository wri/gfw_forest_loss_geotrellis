package org.globalforestwatch.layers

class MexicoProtectedAreas(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/mex_protected_areas/${grid}.tif"
}
