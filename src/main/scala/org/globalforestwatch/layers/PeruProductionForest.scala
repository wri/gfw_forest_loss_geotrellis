package org.globalforestwatch.layers

class PeruProductionForest(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/per_permanent_production_forests/${grid}.tif"
}
