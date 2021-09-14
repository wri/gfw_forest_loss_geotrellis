package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile
import org.globalforestwatch.config.GfwConfig

case class IntactForestLandscapes(gridTile: GridTile)
  extends StringLayer
    with OptionalILayer {
  val uri: String = uriForGrid(GfwConfig.get.rasterLayers("IntactForestLandscapes"), gridTile)

  def lookup(value: Int): String = value match {
    case 0 => ""
    case _ => value.toString

  }
}


case class IntactForestLandscapes2000(gridTile: GridTile)
  extends BooleanLayer
    with OptionalILayer {
  val uri: String = uriForGrid(GfwConfig.get.rasterLayers("IntactForestLandscapes"), gridTile)

  override def lookup(value: Int): Boolean = {
    value match {
      case 0 => false
      case _ => true
    }
  }
}


case class IntactForestLandscapes2013(gridTile: GridTile)
  extends BooleanLayer
    with OptionalILayer {
  val uri: String = uriForGrid(GfwConfig.get.rasterLayers("IntactForestLandscapes"), gridTile)

  override def lookup(value: Int): Boolean = {
    value match {
      case 2016 => true
      case 2013 => true
      case _ => false
    }
  }
}


case class IntactForestLandscapes2016(gridTile: GridTile)
  extends BooleanLayer
    with OptionalILayer {
  val uri: String = uriForGrid(GfwConfig.get.rasterLayers("IntactForestLandscapes"), gridTile)

  override def lookup(value: Int): Boolean = {
    value match {
      case 2016 => true
      case _ => false
    }
  }

}
