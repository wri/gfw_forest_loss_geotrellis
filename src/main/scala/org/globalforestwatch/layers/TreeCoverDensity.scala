package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile
import org.globalforestwatch.config.GfwConfig

trait TreeCoverDensityThreshold extends IntegerLayer with RequiredILayer {

  override val externalNoDataValue: Integer = 0

  override def lookup(value: Int): Integer = {
    value match {
      case v if v <= 10 => 0
      case v if v <= 15 => 10
      case v if v <= 20 => 15
      case v if v <= 25 => 20
      case v if v <= 30 => 25
      case v if v <= 50 => 30
      case v if v <= 75 => 50
      case _ => 75
    }
  }
}

case class TreeCoverDensityThreshold2000(gridTile: GridTile, kwargs: Map[String, Any])
  extends TreeCoverDensityThreshold {
  val datasetName = "umd_tree_cover_density_2000"
  val uri: String =
    uriForGrid(gridTile)
}

case class TreeCoverDensityThreshold2010(gridTile: GridTile, kwargs: Map[String, Any])
  extends TreeCoverDensityThreshold {
  val datasetName = "umd_tree_cover_density_2010"

  val uri: String =
    uriForGrid(gridTile)
}

case class TreeCoverDensity2010_60(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with RequiredILayer {
  val datasetName = "umd_tree_cover_density_2010"
  val uri: String =
    uriForGrid(gridTile)

  override def lookup(value: Int): Boolean = value > 60
}

case class TreeCoverDensityPercent2000(gridTile: GridTile, kwargs: Map[String, Any])
  extends IntegerLayer
    with RequiredILayer {
  val datasetName = "umd_tree_cover_density_2000"
  override val externalNoDataValue: Integer = 0

  val uri: String =
    uriForGrid(gridTile)
}

case class TreeCoverDensityPercent2010(gridTile: GridTile, kwargs: Map[String, Any])
  extends IntegerLayer
    with RequiredILayer {
  val datasetName = "umd_tree_cover_density_2010"
  override val externalNoDataValue: Integer = 0

  val uri: String =
    s"$basePath/umd_tree_cover_density_2010/v1.6/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/percent/gdal-geotiff/${gridTile.tileId}.tif"
}
