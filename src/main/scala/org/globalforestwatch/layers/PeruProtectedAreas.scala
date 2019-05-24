package org.globalforestwatch.layers

case class PeruProtectedAreas(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"$basePath/per_protected_areas/$grid.tif"
}
