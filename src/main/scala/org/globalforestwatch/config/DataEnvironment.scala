package org.globalforestwatch.config

import io.circe.Json
import org.globalforestwatch.grids.GridTile
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.syntax._
import org.globalforestwatch.config

case class DataEnvironment(layers: List[LayerConfig]) {
  def getSourceUri(dataset: String, grid: GridTile): String = {
    layers
      .find(lyr =>
        lyr.name == dataset &&
          lyr.grid == s"${grid.gridSize}/${grid.rowCount}") match {
      case Some(lyr: LayerConfig) => lyr.source_uri
      case None =>
        throw new IllegalArgumentException(s"No configuration found for dataset ${dataset} on a ${grid} grid")
    }
  }
}

case class LayerConfig(name: String, source_uri: String, grid: String) // raster_table: Option[RasterTable])

case class RasterTable(rows: List[RasterTableRow], default_meaning: Option[String])

case class RasterTableRow(value: Integer, meaning: Either[Int, String])

object DataEnvironment {
  def getDataEnvironment(configPath: String): DataEnvironment = {
    val rawJson = scala.io.Source.fromFile(configPath).mkString
    print("here")
    decode[DataEnvironment](rawJson) match {
      case Left(exc) =>
        println(s"Invalid data environment json: ${rawJson}")
        throw exc
      case Right(value) => value
    }
  }

//  def getDataEnvironment(configPath: String): DataEnvironment = {
//    val rawJson: Json = scala.io.Source.fromFile(configPath).mkString
//    getDataEnvironment(rawJson)
//  }
}