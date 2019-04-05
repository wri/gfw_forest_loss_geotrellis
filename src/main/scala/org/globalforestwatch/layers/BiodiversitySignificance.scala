package org.globalforestwatch.layers

class BiodiversitySignificance(grid: String) extends IntegerLayer with OptionalDLayer {
  val uri: String = s"s3://$basePath/biodiversity_significance/$grid.tif"

  def lookup(value: Double): Integer = value match {
  ???
  }
}