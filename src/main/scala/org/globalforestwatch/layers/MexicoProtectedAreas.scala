package org.globalforestwatch.layers

case class MexicoProtectedAreas(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"$basePath/mex_protected_areas/$grid.tif"
}
