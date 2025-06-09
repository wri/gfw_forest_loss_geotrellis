package org.globalforestwatch.config

import org.globalforestwatch.grids.GridTile
import io.circe.generic.auto._
import io.circe.parser.decode
import org.globalforestwatch.util.Util.jsonStrToMap
import scalaj.http.{Http, HttpOptions, HttpResponse}
import cats.data.NonEmptyList
import org.globalforestwatch.util.Config

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
  def getRasterCatalog(catalogFile: String, pinned: Option[NonEmptyList[Config]]): RasterCatalog = {
    val rawJson = try {
      scala.io.Source.fromResource(catalogFile).getLines.mkString
    } catch {
      case e: Throwable =>
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
          getSourceUri(config.name, config.source_uri, pinned)
        )
      )
    )
  }

  /** Return the sourceUri to be used. If the sourceUri argument has 'latest' as its
   * version, then either use a pinned version, if there is a matching entry for the
   * dataset in pinned, or determine the actual latest version and use that.
   */
  def getSourceUri(dataset: String, sourceUri: String, pinned: Option[NonEmptyList[Config]]): String = {
    // If we are skipping the use of GDAL, then replace any 'gdal-geotiff' folder in
    // the URI with 'geotiff', since we won't be able to deal with special
    // GDAL-optimized tiffs. This is a fairly special case, but it's only for local
    // use where people don't want to set up the correct GDAL version.
    val sourceUri2 = 
      if (GfwConfig.skipGdalFlag) {
        sourceUri.replace("/gdal-geotiff/", "/geotiff/")
      } else {
        sourceUri
      }
    if (sourceUri2.contains("latest")) {
      pinned match {
        case Some(list) => {
          list.toList.foreach(c => {
            if (c.key == dataset) {
              return sourceUri2.replace("latest", c.value)
            }
          })
        }
        case None =>
      }
      sourceUri2.replace("latest", getLatestVersion(dataset))
    } else {
      sourceUri2
    }
  }

  // Internal function to do a "latest" request to the Data API, doing retries
  // recursively as necessary. Return the JSON response. Throws an exception if there
  // was no successful response.
  def getLatestVersionResponse(dataset: String, retries: Int = 0): HttpResponse[String] = {
    println("Sending data API 'latest' request")
    val response: HttpResponse[String] = Http(
      s"https://data-api.globalforestwatch.org/dataset/${dataset}/latest"
    ).option(HttpOptions
      .followRedirects(true))
      .option(HttpOptions.connTimeout(20000))
      .option(HttpOptions.readTimeout(50000)).asString
    println("Got data API response")

    if (response.isSuccess) {
      response
    } else if (retries > 2) {
      // Do 3 total tries to get a successful response, then throw an exception.
      throw new IllegalArgumentException(
        s"Problem accessing latest version of dataset ${dataset}. Data API response code: ${response.code}"
      )
    } else {
      println(s"Retrying after problem accessing latest version of dataset ${dataset}. Data API response code: ${response.code}")
      // Sleep for ten seconds and then return any successful retry.
      Thread.sleep(10000)
      getLatestVersionResponse(dataset, retries + 1)
    }
  }

  /** Get the latest version of a dataset, by calling the Data API.  Do several retries if there are errors.
    */
  def getLatestVersion(dataset: String): String = {
    val response = getLatestVersionResponse(dataset)

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
