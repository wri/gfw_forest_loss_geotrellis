package org.globalforestwatch.config

import io.circe.Json
import org.globalforestwatch.grids.GridTile
import io.circe.generic.auto._
import io.circe.syntax._

case class DataEnvironment(layers: List[LayerConfig])

case class LayerConfig(dataset: String, version: String, sourceUri: String, grid: String, rasterTable: RasterTable)

case class RasterTable(rows: List[RasterTableRow], defaultMeaning: String)

case class RasterTableRow(value: Integer, meaning: String)

object DataEnvironment {
  val rawJson: Json
  val layers: DataEnvironment = {
    rawJson.as[DataEnvironment] match {
      case Left(exc) =>
        println(s"Invalid data environment json: ${rawJson}")
        throw exc
      case Right(value) => value
    }
  }

  def getSourceUri(dataset: String, grid: GridTile): String = {
    layers.layers
      .find(lyr =>
        lyr.dataset == dataset &&
          lyr.grid == s"${grid.gridSize}/${grid.blockSize}") match {
      case Some(lyr: LayerConfig) => lyr.sourceUri
      case None =>
        throw new IllegalArgumentException(s"No configuration found for dataset ${dataset} on a ${grid} grid")
    }
  }
}