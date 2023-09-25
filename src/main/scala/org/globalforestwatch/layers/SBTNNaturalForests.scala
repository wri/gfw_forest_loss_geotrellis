package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class SBTNNaturalForests(gridTile: GridTile, kwargs: Map[String, Any])
    extends StringLayer
        with OptionalILayer {

    val datasetName = "sbtn_natural_forests_map"
    val uri: String = uriForGrid(gridTile, kwargs)

    override val externalNoDataValue = "Unknown"

    def lookup(value: Int): String = value match {
        case 0 => "Non-Forest"
        case 1 => "Natural Forest"
        case 2 => "Non-Natural Forest"
        case _ => "Unknown"
    }
}