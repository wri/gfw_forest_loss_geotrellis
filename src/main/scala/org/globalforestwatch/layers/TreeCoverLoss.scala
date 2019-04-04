package org.globalforestwatch.layers

class TreeCoverLoss(grid: String) extends IntegerLayer with RequiredILayer {
  val uri: String = s"s3://wri-users/tmaschler/prep_tiles/loss/${grid}.tif"
  override def lookup(value: Int): Integer = {
    value + 2000
  }
}
