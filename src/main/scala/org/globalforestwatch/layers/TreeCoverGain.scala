package org.globalforestwatch.layers

class TreeCoverGain(grid: String) extends IntegerLayer with RequiredILayer {
  val uri: String = s"s3://wri-users/tmaschler/prep_tiles/gain/${grid}.tif"

  override def lookup(value: Int): Integer = if (value == 0) 0 else 1
}
