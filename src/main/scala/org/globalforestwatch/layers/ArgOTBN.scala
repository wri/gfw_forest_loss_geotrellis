package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ArgOTBN(gridTile: GridTile, kwargs: Map[String, Any])
    extends StringLayer
        with OptionalILayer {

    val datasetName = "arg_native_forest_land_plan"
    val uri: String = 
        uriForGrid(gridTile)

    override val externalNoDataValue = "Unknown"

    def lookup(value: Int): String = value match {
        case 1 => "Category I"
        case 2 => "Category II"
        case 3 => "Category III"
        case _ => "Unknown"
    }
}
