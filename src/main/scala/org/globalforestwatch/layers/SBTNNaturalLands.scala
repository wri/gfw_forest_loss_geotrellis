package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class SBTNNaturalLands(gridTile: GridTile, kwargs: Map[String, Any])
    extends IntegerLayer
      with OptionalILayer {

    val datasetName = "sbtn_natural_lands_classification"
    val uri: String = uriForGrid(gridTile, kwargs)

    override val externalNoDataValue = 0

}

object SBTNNaturalLands {
  def isNaturalForest(v: Integer): Boolean = (v == 2 || v == 5 || v == 8 || v == 9)
  def isNaturalOpenEcosystem(v: Integer): Boolean = (v == 3 || v == 4 || v == 6 || v == 10 || v == 11)
}
