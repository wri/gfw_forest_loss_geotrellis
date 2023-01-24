package org.globalforestwatch.config

import io.circe.Json
import org.globalforestwatch.grids.GridTile
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.syntax._
import org.globalforestwatch.config
import org.globalforestwatch.util.Util.jsonStrToMap
import scalaj.http.{Http, HttpOptions, HttpResponse}

import java.io.FileNotFoundException

case class RasterCatalog(layers: List[LayerConfig]) {
  def getSourceUri(dataset: String, grid: GridTile): String = {
    //  lyr.grid == s"${grid.gridSize}/${grid.rowCount}"
    layers.find(lyr => lyr.name == dataset) match {
      case Some(lyr: LayerConfig) => lyr.source_uri
      case None =>
        throw new IllegalArgumentException(s"No configuration found for dataset ${dataset} on a ${grid} grid")
    }
  }
}

case class LayerConfig(name: String, source_uri: String)

object RasterCatalog {
  def getRasterCatalog(catalogFile: String): RasterCatalog = {
    val rawJson = try {
      scala.io.Source.fromResource(catalogFile).getLines.mkString
    } catch {
      case e =>
        throw new FileNotFoundException(s"Cannot open raster catalog ${catalogFile}: ${e.getMessage}")
    }

    val parsed = decode[RasterCatalog](rawJson) match {
      case Left(exc) =>
        println(s"Invalid data environment json: ${rawJson}")
        throw exc
      case Right(value) => value
    }

    RasterCatalog(
      parsed.layers.map((config: LayerConfig) =>
        LayerConfig(
          config.name,
          getSourceUri(config.name, config.source_uri)
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

  def getLatestVersion(dataset: String, retries: Int = 0): String = {
    val response: HttpResponse[String] = Http(
      s"https://data-api.globalforestwatch.org/dataset/${dataset}/latest"
    ).option(HttpOptions
      .followRedirects(true))
      .option(HttpOptions.connTimeout(10000))
      .option(HttpOptions.readTimeout(50000)).asString

    if (!response.isSuccess) {
      if (retries <= 2) {
        Thread.sleep(10000)
        getLatestVersion(dataset, retries + 1)
      } else {
        throw new IllegalArgumentException(
          s"Dataset ${dataset} has no latest version or does not exist. Data API response code: ${response.code}"
        )
      }
    }

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