package org.globalforestwatch.layers

class BiodiversitySignificance(grid: String) extends StringLayer with OptionalILayer {
  val uri: String = s"s3://$basePath/biodiversity_significance/$grid.tif"

  def lookup(value: Float): String = value match {
  ???
  }
}