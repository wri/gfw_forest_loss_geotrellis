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

  def toVectorTileFeature: Feature[Polygon, Map[String, vectortile.Value]] = {
    val attributes = for {
      hist <- histogram
      (min, max) <- hist.minMaxValues()
    } yield Map("elevation_min" -> vectortile.VDouble(min), "elecation_max" -> vectortile.VDouble(max))

    Feature(footprint, attributes.getOrElse(Map.empty))
  }

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
  def readFromGeoJson(url: URL): Iterator[Polygon] = {
    // TODO: consider how bad it is to leave the InputStream open
    // TODO: consider using is.seek(n) to partition reading the list
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
    stream.iterator().asScala.flatMap {
      case FeatureRx(json) =>
        val poly = json.parseGeoJson[Polygon]
        idx += 1
        if (poly.isValid)
          Some(poly)
        else {
          logger.warn(s"Dropping invalid geometry: ${poly.toWKT}")
          None
        }
      case _ =>
        None
    }
  }

  def geoJsonURLs: List[String] = List(
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Alabama.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Alaska.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Arizona.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Arkansas.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/California.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Colorado.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Connecticut.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Delaware.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/DistrictofColumbia.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Florida.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Georgia.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Hawaii.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Idaho.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Illinois.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Indiana.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Iowa.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Kansas.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Kentucky.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Louisiana.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Maine.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Maryland.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Massachusetts.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Michigan.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Minnesota.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Mississippi.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Missouri.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Montana.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Nebraska.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Nevada.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/NewHampshire.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/NewJersey.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/NewMexico.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/NewYork.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/NorthCarolina.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/NorthDakota.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Ohio.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Oklahoma.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Oregon.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Pennsylvania.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/RhodeIsland.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/SouthCarolina.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/SouthDakota.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Tennessee.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Texas.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Utah.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Vermont.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Virginia.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Washington.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/WestVirginia.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Wisconsin.zip",
    "https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Wyoming.zip")
}
