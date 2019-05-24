package com.globalforestwatch.util

import java.io.{
  BufferedReader,
  FileNotFoundException,
  InputStream,
  InputStreamReader
}
import java.net.URL
import java.security.InvalidParameterException
import java.util.zip.ZipInputStream

import geotrellis.vector._
import geotrellis.vector.{Feature, Geometry, Polygon}
import org.apache.log4j.Logger

import scala.collection.JavaConverters._

object GeoJsonUtil {
  val logger = Logger.getLogger(this.getClass)

  // Greedy match, will trim white space around but won't ensure proper GeoJSON
  val FeatureRx = """.*(\{\"type\":\"Feature\".+}).*""".r

  /** Read GeoJSON file containing FeatureCollection, one line per feature.
    *
    * Return of this function is an Iterator to avoid allocating memory for full data set at once.
    * Example: California.geojson is 2.66GB uncompressed
    *
    * Supports: .zip, .json, .geojson files
    */
  def readFromGeoJson(url: URL): Iterator[Geometry] = {
    val is: InputStream = url.getPath match {
      case null =>
        throw new FileNotFoundException(s"Can't read")

      case p if p.endsWith(".geojson") || p.endsWith(".json") =>
        url.openStream()

      case p if p.endsWith(".zip") =>
        val zip = new ZipInputStream(url.openStream)
        val entry = zip.getNextEntry
        logger.info(s"Reading: $url - ${entry.getName}")
        zip

      case _ =>
        throw new InvalidParameterException(s"Can't read: $url format")
    }

    val reader: BufferedReader = new BufferedReader(new InputStreamReader(is))
    val stream = reader.lines()
    var idx: Int = 0

    val FileNameRx = """.*\/(\w+)\.\w+""".r
    val FileNameRx(name) = url.getFile

    stream.iterator().asScala.flatMap {
      case FeatureRx(json) =>
        val geom = json.parseGeoJson[Geometry]
        idx += 1
        if (geom.isValid)
          Some(geom)
        else {
          // Invalid geometry can't be checked for intersection, they are sadly inevitable
          logger.warn(s"Dropping invalid geometry: ${geom.toWKT}")
          None
        }
      case _ =>
        None
    }
  }
}
