package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class PlantationsTypeFluxModel(gridTile: GridTile, kwargs: Map[String, Any])
  extends StringLayer
    with OptionalILayer {

  val datasetName = "Na"

  val uri: String =
    s"s3://gfw-files/flux_1_2_0/plantation_type/standard/${gridTile.tileId}.tif"

  override val externalNoDataValue = "Not applicable"

  def lookup(value: Int): String = value match {
    case 1 => "Oil palm"
    case 2 => "Wood fiber"
    case 3 => "Other"
    case _ => "Unknown"
  }
}
