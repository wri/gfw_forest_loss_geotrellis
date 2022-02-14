package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile
import org.globalforestwatch.config.GfwConfig

case class IntactForestLandscapes(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {
  val datasetName = "ifl_intact_forest_landscapes"
  val uri: String =
    uriForGrid(gridTile)


  def lookup(value: Int): String = value match {
    case 0 => ""
    case _ => value.toString

  }
}

case class IntactForestLandscapes2000(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "ifl_intact_forest_landscapes"
  val uri: String =
    uriForGrid(gridTile)

  override def lookup(value: Int): Boolean = {
    value match {
      case 0 => false
      case _ => true
    }
  }
}

case class IntactForestLandscapes2013(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "ifl_intact_forest_landscapes"
  val uri: String =
    uriForGrid(gridTile)

  override def lookup(value: Int): Boolean = {
    value match {
      case 2016 => true
      case 2013 => true
      case _ => false
    }
  }
}

case class IntactForestLandscapes2016(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {
  val datasetName = "ifl_intact_forest_landscapes"
  val uri: String =
    uriForGrid(gridTile)

  override def lookup(value: Int): Boolean = {
    value match {
      case 2016 => true
      case _ => false
    }
  }

}
