package org.globalforestwatch.layers

class RSPO(grid: String) extends StringLayer with OptionalILayer {

  val uri: String = s"$basePath/rspo/$grid.tif"

  def lookup(value: Int): String = value match {
    case 1 => "certified"
    case 2 => "unknown"
    case 3 => "not certified"
  }
}
