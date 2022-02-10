package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile
import org.globalforestwatch.config.GfwConfig

case class ProtectedAreas(gridTile: GridTile) extends StringLayer with OptionalILayer {

  val uri: String = uriForGrid(GfwConfig.get.rasterLayers(getClass.getSimpleName()), gridTile)

  def lookup(value: Int): String = value match {
    case 1 => "Category Ia/b or II"
    case 2 => "Other Category"
    case _ => ""
  }
}
