package org.globalforestwatch.layers

case class IntactPrimaryForest(grid: String, model: String="standard")
    extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"$basePath/ifl_primary/$model/$grid.tif"
}
