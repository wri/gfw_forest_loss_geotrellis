package org.globalforestwatch.layers

class PeruProductionForest(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"$basePath/per_permanent_production_forests/$grid.tif"
}
