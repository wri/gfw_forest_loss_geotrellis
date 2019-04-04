package org.globalforestwatch.layers

class PeruProtectedAreas(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/per_protected_areas/${grid}.tif"
}
