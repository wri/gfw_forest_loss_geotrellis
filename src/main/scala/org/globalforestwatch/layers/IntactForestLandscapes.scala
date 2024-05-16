package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class IntactForestLandscapes(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {
  val datasetName = "ifl_intact_forest_landscapes"
  val uri: String =
    uriForGrid(gridTile, kwargs)


  def lookup(value: Int): String = value match {
    case 0 => ""
    case _ => value.toString

  }
}

case class IntactForestLandscapes2000(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer{
  val datasetName = "ifl_intact_forest_landscapes_2000"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}

case class IntactForestLandscapes2013(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer{
  val datasetName = "ifl_intact_forest_landscapes_2013"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}

case class IntactForestLandscapes2016(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer{
  val datasetName = "ifl_intact_forest_landscapes_2016"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}

case class IntactForestLandscapes2020(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer{
  val datasetName = "ifl_intact_forest_landscapes_2020"
  val uri: String =
    uriForGrid(gridTile, kwargs)
}
