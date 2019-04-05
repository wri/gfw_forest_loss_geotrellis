package org.globalforestwatch.layers

class ProtectedAreas(grid: String) extends StringLayer with OptionalILayer {

  val uri: String = s"$basePath/prep_tiles/wdpa/$grid.tif"

  def lookup(value: Int): String = value match {
    case 1 => "Category Ia/b or II"
    case 2 => "Other Category"
  }
}
