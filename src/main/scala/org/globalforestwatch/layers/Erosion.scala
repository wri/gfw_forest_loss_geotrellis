package org.globalforestwatch.layers

class Erosion(grid: String) extends StringLayer with OptionalILayer {
  val uri: String = s"$basePath/erosion/$grid.tif"

  def lookup(value: Int): String = value match {
  ???
  }
}