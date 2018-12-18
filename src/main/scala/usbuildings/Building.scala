package usbuildings

import java.io.FileNotFoundException
import java.net.URL
import java.security.InvalidParameterException

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.histogram.Histogram
import geotrellis.vector.{Feature, Polygon}
import geotrellis.vectortile

import scala.collection.JavaConverters._


/** Part of building SimpleFeature we care about
  * Note: case class provides safe equality testing
  */
case class Building(file: String, idx: Int)(
  val footprint: Polygon,
  val histogram: Option[Histogram[Double]] = None
) {
  def id: (String, Int) = (file, idx)

  def toVectorTileFeature: Feature[Polygon, Map[String, vectortile.Value]] = ???

  def withHistogram(hist: Histogram[Double]): Building = {
    copy()(footprint, Some(hist))
  }

  def withFootprint(poly: Polygon): Building = {
    copy()(poly, histogram)
  }

  def mergeHistograms(other: Building): Building = {
    require(other.idx == idx)
    val hist = {
      val mergedHist = for (h1 <- histogram; h2 <- other.histogram) yield h1 merge h2
      mergedHist.orElse(histogram).orElse(other.histogram)
    }
    withHistogram(hist.get)
  }
}

/** Container for per building result
  * Needs to support combine method so we can reduce over partial results
  * None min/max is possible of building does not intersect suitable DEM
  */
case class Elevation(min: Double, max: Double) {
  private def withNaN(f: (Double, Double) => Double, x: Double, y: Double): Double = {
    if (y.isNaN) x
    else if (x.isNaN) x
    else f(x, y)
  }
  def combine(other: Elevation) =
    Elevation(withNaN(math.min, min, other.min), withNaN(math.max, max, other.max))
}

object Building extends LazyLogging {
  import java.io.{BufferedReader, InputStream, InputStreamReader}
  import java.util.zip.ZipInputStream

  import geotrellis.vector.Polygon
  import geotrellis.vector.io._

  // Greedy match, will trim white space around but won't ensure proper GeoJSON
  val FeatureRx = """.*(\{\"type\":\"Feature\".+}).*""".r

  /**
    * California.geojson is 2.66GB uncompressed, need to read it as a stream to avoid blowing the heap\
    * Supports: .zip, .json, .geojson files
  */
  def readFromGeoJson(url: URL): Iterator[Building] = {
    // TODO: consider how bad it is to leave the InputStream open
    // TODO: consider using is.seek(n) to partition reading the list
    val is: InputStream = url.getPath match {
      case null =>
        throw new FileNotFoundException("Can't read")

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
    stream.iterator().asScala.flatMap {
      case FeatureRx(json) =>
        val poly = json.parseGeoJson[Polygon]
        idx += 1
        if (poly.isValid)
          Some(Building(url.getFile, idx)(poly))
        else {
          logger.warn(s"Dropping invalid geometry: ${poly.toWKT}")
          None
        }
      case _ =>
        None
    }
  }
}
