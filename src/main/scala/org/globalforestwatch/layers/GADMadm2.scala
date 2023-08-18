package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GadmAdm2(gridTile: GridTile, kwargs: Map[String, Any]) 
    extends IntegerLayer
        with OptionalILayer {
        
        val datasetName: String = "gadm_adm2"
        val uri: String = 
            uriForGrid(gridTile, kwargs)

        override def lookup(value: Int): Integer = 
            if (value == 9999) null else value
        
            
        }
    