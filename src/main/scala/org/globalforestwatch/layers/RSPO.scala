package org.globalforestwatch.layers

case class RSPO(grid: String) extends StringLayer with OptionalILayer {

  val uri: String = s"$basePath/rspo/$grid.tif"

  def lookup(value: Int): String = value match {
    case 1 => "Certified"
    case 2 => "Unknown"
    case 3 => "Not certified"
    case _ => ""
  }
}
