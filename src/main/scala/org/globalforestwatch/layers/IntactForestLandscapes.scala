package org.globalforestwatch.layers

case class IntactForestLandscapes(grid: String)
  extends StringLayer
    with OptionalILayer {
  val uri: String = s"$basePath/ifl/$grid.tif"

  def lookup(value: Int): String = value match {
    case 0 => ""
    case _ => value.toString

  }
}

case class IntactForestLandscapes2016(grid: String)
  extends BooleanLayer
    with OptionalILayer {
  val uri: String = s"$basePath/ifl/$grid.tif"

  override def lookup(value: Int): Boolean = {
    value match {
      case 2016 => true
      case _ => false
    }
  }

}
