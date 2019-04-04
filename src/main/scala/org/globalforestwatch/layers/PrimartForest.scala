package org.globalforestwatch.layers

class PrimaryForest(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/primary_forest/${grid}.tif"
}
