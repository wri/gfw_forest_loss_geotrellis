package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class NegligibleRisk(gridTile: GridTile, kwargs: Map[String, Any])
    extends StringLayer
        with OptionalILayer {

    val datasetName = "gfwpro_negligible_risk_analysis"
    val uri: String = 
        uriForGrid(gridTile, kwargs)

    def lookup(value: Int): String = value match {
        case 1 => "NO"
        case 2 => "YES"
        case 3 => "NA"
        case _ => "Unknown"
    }
}
