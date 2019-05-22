package org.globalforestwatch.layers

case class IntactForestLandscapes(grid: String)
  extends StringLayer
    with OptionalILayer {
  val uri: String = s"$basePath/ifl/$grid.tif"

  def lookup(value: Int): String = value match {
    case 0 => "",
    case _ => value.toString

  }
}
