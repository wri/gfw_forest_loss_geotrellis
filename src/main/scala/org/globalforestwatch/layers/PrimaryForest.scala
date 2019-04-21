package org.globalforestwatch.layers

class PrimaryForest(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/primary_forest/$grid.tif"
}
