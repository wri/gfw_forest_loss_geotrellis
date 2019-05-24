package org.globalforestwatch.layers

case class PrimaryForest(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"$basePath/primary_forest/$grid.tif"
}
