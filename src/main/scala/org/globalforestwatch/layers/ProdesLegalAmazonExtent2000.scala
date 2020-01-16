package org.globalforestwatch.layers

case class ProdesLegalAmazonExtent2000(grid: String) extends BooleanLayer with OptionalILayer {
  val uri: String = s"$basePath/legal_Amazon_2000/$grid.tif"
}
