package org.globalforestwatch.layers

class Erosion(grid: String) extends StringLayer with OptionalILayer {
  val uri: String = s"s3://wri-users/tmaschler/prep_tiles/erosion/${grid}.tif"

  def lookup(value: Int): String = value match {
  ???
  }
}