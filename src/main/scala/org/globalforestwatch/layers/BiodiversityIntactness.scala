package org.globalforestwatch.layers

class BiodiversityIntactness(grid: String) extends IntegerLayer with OptionalDLayer {
  val uri: String = s"$basePath/biodiversity_intactness/$grid.tif"

  def lookup(value: Double): Integer = value match {
  ???
  }
}