package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class ProdesCerradoLossYear(gridTile: GridTile, kwargs: Map[String, Any]) extends IntegerLayer with OptionalILayer {
  val datasetName = "inpe_cerrado_prodes"
  val uri: String =
    uriForGrid(gridTile, kwargs)

  override def lookup(value: Int): Integer =
    if (value == 0) null else value
}
