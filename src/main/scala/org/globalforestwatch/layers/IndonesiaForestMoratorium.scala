package org.globalforestwatch.layers

class IndonesiaForestMoratorium(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String =
    s"s3://wri-users/tmaschler/prep_tiles/idn_forest_moratorium/${grid}.tif"
}
