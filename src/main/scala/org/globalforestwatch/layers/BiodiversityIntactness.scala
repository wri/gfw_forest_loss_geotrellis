package org.globalforestwatch.layers

class BiodiversityIntactness(grid: String) extends StringLayer with OptionalILayer {
  val uri: String = s"$basePath/biodiversity_intactness/$grid.tif"

  def lookup(value: Float): String = value match {
  ???
  }
}