package org.globalforestwatch.layers

case class IntactPrimaryForest(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"$basePath/ifl_primary/$grid.tif"
}
