package org.globalforestwatch.config

import io.circe.Json
import org.globalforestwatch.grids.GridTile
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.syntax._
import org.globalforestwatch.config
import org.globalforestwatch.util.Util.jsonStrToMap
import scalaj.http.{Http, HttpOptions, HttpResponse}

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

case class LayerConfig(name: String, source_uri: String, grid: String)

object DataEnvironment {
  def getDataEnvironment(configPath: String): DataEnvironment = {
    val rawJson = scala.io.Source.fromFile(configPath).mkString

    val parsed = decode[DataEnvironment](rawJson) match {
      case Left(exc) =>
        println(s"Invalid data environment json: ${rawJson}")
        throw exc
      case Right(value) => value
    }

    DataEnvironment(
      parsed.layers.map((config: LayerConfig) =>
        LayerConfig(
          config.name,
          getSourceUri(config.name, config.source_uri),
          config.grid
        )
      )
    )
  }

  def getSourceUri(dataset: String, sourceUri: String): String = {
    if (sourceUri.contains("latest")) {
      sourceUri.replace("latest", getLatestVersion(dataset))
    } else {
      sourceUri
    }
  }

  def getLatestVersion(dataset: String): String = {
    val response: HttpResponse[String] = Http(
      s"https://data-api.globalforestwatch.org/dataset/${dataset}/latest"
    ).option(HttpOptions
      .followRedirects(true)).option(HttpOptions.connTimeout(10000)).option(HttpOptions.readTimeout(50000)).asString

    if (response.code != 200)
      throw new IllegalArgumentException(
        s"Dataset ${dataset} has no latest version or does not exit. Data API response code: ${response.code}"
      )

    val json: Map[String, Any] = jsonStrToMap(response.body)

    val data = json.get("data").asInstanceOf[Option[Map[String, Any]]]
    data match {
      case Some(map) =>
        val version = map.get("version").asInstanceOf[Option[String]]
        version match {
          case Some(value) => value
          case _ => throw new RuntimeException("Cannot understand Data API response.")
        }
      case _ => throw new RuntimeException("Cannot understand Data API response.")
    }
  }
}