package org.globalforestwatch.layers

case class ProtectedAreas(grid: String) extends StringLayer with OptionalILayer {

  val uri: String = s"$basePath/wdpa/$grid.tif"

  def lookup(value: Int): String = value match {
    case 1 => "Category Ia/b or II"
    case 2 => "Other Category"
    case _ => null
  }
}
