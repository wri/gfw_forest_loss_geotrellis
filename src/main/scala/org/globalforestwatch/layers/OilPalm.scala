package org.globalforestwatch.layers

class OilPalm(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"s3://wri-users/tmaschler/prep_tiles/oil_palm/${grid}.tif"
}
