package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class NegligibleRisk(gridTile: GridTile, kwargs: Map[String, Any])
    extends StringLayer
        with OptionalILayer {

    val datasetName = "gfwpro_negligible_risk"
    val uri: String = 
        uriForGrid(gridTile, kwargs)

    override val externalNoDataValue = "Unknown"

    def lookup(value: Int): String = value match {
        case 0 => "NO"
        case 1 => "YES"
        case 2 => "NA"
        case _ => "Unknown"
    }
}
