package org.globalforestwatch.layers

class IndonesiaPrimaryForest(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/idn_primary_forest/${grid}.tif"
}
