package org.globalforestwatch.layers

class KeyBiodiversityAreas(grid: String)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String = s"s3://wri-users/tmaschler/prep_tiles/kba/${grid}.tif"
}
