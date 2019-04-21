package org.globalforestwatch.layers

class IndonesiaPrimaryForest(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"$basePath/idn_primary_forest/$grid.tif"
}
