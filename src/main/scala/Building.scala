package geotrellis.demo.footprint

import java.net.URL
import java.security.InvalidParameterException

import com.typesafe.scalalogging.LazyLogging
import geotrellis.vector.{Feature, MultiPolygon}
import geotrellis.geotools._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{DataStore, DefaultTransaction}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.data.simple.{SimpleFeatureIterator, SimpleFeatureStore}
import org.geotools.referencing.CRS
import com.vividsolutions.jts
import geotrellis.vector.io._

import scala.collection.JavaConverters._
import scala.util.control.NonFatal


/** Part of building SimpleFeature we care about
  * Note: case class provides safe equality testing
  */
case class Building(uuid: String, footprint: MultiPolygon)

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
  /** Read building geometries into RDD.
    * Each layer will become its own partition in the RDD.
    *
    * @param uri    URI for GeoTools DataStore
    * @param layers List of layers to include, all layers if this parameter is empty
    */
  def getRDD(uri: String, layers: List[String] = List.empty)(implicit sc: SparkContext): RDD[Building] = {
    val buildingLayers: Seq[String] = {
      val ds: DataStore = Util.getOgrDataStore(uri)
      ds.getNames
        .asScala
        .map(_.getLocalPart)
        .filter { name => layers.isEmpty || layers.contains(name) }
    }
    require(buildingLayers.nonEmpty, s"Building layers list is empty")

    sc.parallelize(buildingLayers, buildingLayers.length).flatMap { layer =>
      // This will read entire layer in a single partition
      // Ideally the reading process could split amongst many executors per layer,
      // however there is a limitation in GeoTools that prevents BBOX query on layers without FID attribute
      // weirdly building SimpleFeatures have FID_1 attribute instead
      val features: SimpleFeatureIterator =
        Util.getOgrDataStore(uri).getFeatureSource(layer).getFeatures.features()

      new Iterator[Building] { // wrap GeoTools iterator into Scala iterator
        def hasNext: Boolean = features.hasNext
        def next: Building = {
          val sf: SimpleFeature = features.next()
          // Convert GeoTools Feature to GeoTrellis Feature
          val feature: Feature[MultiPolygon, Map[String, Any]] = sf.toFeature[MultiPolygon]
          Building(feature.data("uniqueid").asInstanceOf[String], feature.geom)
        }
      }
    }
  }

  val resultSimpleFeatureType: SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.setName("FootprintResult")
    builder.setNamespaceURI("http://www.geotools.org/")
    builder.add("the_geom", classOf[jts.geom.MultiPolygon])
    builder.add("uniqueid", classOf[String])
    builder.add("Shape_Min", classOf[java.lang.Double])
    builder.add("Shape_Max", classOf[java.lang.Double])
    builder.setCRS(CRS.decode("EPSG:4326"))
    builder.buildFeatureType()
  }

  def toSimpleFeature(building: Building, result: Elevation): SimpleFeature = {
    val sfb = new SimpleFeatureBuilder(resultSimpleFeatureType)
    sfb.add(building.footprint.jtsGeom)
    sfb.add(building.uuid)
    sfb.add(result.min)
    sfb.add(result.max)
    sfb.buildFeature(building.uuid)
  }

  def writeToShapefile(url: String, results: Iterator[(Building, Elevation)]): Unit = {
    val dataStoreFactory = new ShapefileDataStoreFactory
    val params = new java.util.HashMap[java.lang.String, java.io.Serializable]
    params.put("url", url)
    params.put("create spatial index", true)
    val newDataStore = dataStoreFactory.createNewDataStore(params).asInstanceOf[ShapefileDataStore]
    newDataStore.createSchema(resultSimpleFeatureType)
    val transaction = new DefaultTransaction("create")
    val featureStore = newDataStore.getFeatureSource().asInstanceOf[SimpleFeatureStore]
    featureStore.setTransaction(transaction)

    val fc = new DefaultFeatureCollection()
    results.foreach { case (b, r) => fc.add(toSimpleFeature(b, r)) }

    try {
      featureStore.addFeatures(fc)
      transaction.commit()
    } catch {
      case NonFatal(problem) =>
        problem.printStackTrace()
        transaction.rollback()
    } finally {
      transaction.close()
    }
  }


  import geotrellis.vector.Polygon
  import java.util.zip.ZipInputStream
  import java.io.{InputStream, InputStreamReader, BufferedReader}
  import geotrellis.vector.io._

  // Greedy match, will trim white space around but won't ensure proper GeoJSON
  val FeatureRx = """.*(\{\"type\":\"Feature\".+}).*""".r

  /**
    * California.geojson is 2.66GB uncompressed, need to read it as a stream to avoid blowing the heap\
    * Supports: .zip, .json, .geojson files
  */
  def readFromFile(url: URL): Iterator[Polygon] = {
    // TODO: consider how bad it is to leave the InputStream open
    // TODO: consider using is.seek(n) to partition reading the list
    val is: InputStream = url.getPath match {
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
    stream.iterator().asScala.flatMap {
      case FeatureRx(json) =>
        val poly = json.parseGeoJson[Polygon]
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
}
