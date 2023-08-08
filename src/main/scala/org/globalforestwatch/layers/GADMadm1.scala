package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GADMadm1(gridTile: GridTile, kwargs: Map[String, Any]) 
    extends IntegerLayer
        with OptionalILayer {
        
        val datasetName: String = "gadm_adm1"
        val uri: String = 
            uriForGrid(gridTile, kwargs)

        override def lookup(value: Int): Integer = 
            if (value == 9999) null else value
        
            
        }
    