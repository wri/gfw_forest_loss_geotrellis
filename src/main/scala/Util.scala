package geotrellis.demo.footprint

import java.io.{File, FileInputStream}

import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.ObjectMetadata
import geotrellis.contrib.vlm.RasterRef
import geotrellis.spark.io.s3.S3Client
import org.geotools.data.ogr.OGRDataStore
import org.geotools.data.ogr.bridj.BridjOGRDataStoreFactory

import scala.util.control.NonFatal

object Util {
  def getOgrDataStore(uri: String, driver: Option[String] =  None): OGRDataStore = {
    println(s"Opening: $uri")
    val factory = new BridjOGRDataStoreFactory()
    val params = new java.util.HashMap[String, String]
    params.put("DatasourceName", uri)
    driver.foreach(params.put("DriverName", _))
    factory.createDataStore(params).asInstanceOf[OGRDataStore]
  }

  /** Join intersecting buildings with their overlapping rasters */
  def joinBuildingsToRasters(
     buildings: Iterable[Building],
     rasters: Iterable[RasterRef]
   ): Map[Building, Seq[RasterRef]] = {
    val intersecting: Seq[(Building, RasterRef)] = {
      for {
        building <- buildings
        dem <- rasters if dem.extent.intersects(building.footprint.envelope)
      } yield (building, dem)
    }.toSeq

    intersecting.groupBy(_._1).mapValues(_.map(_._2))
  }

  def uploadFile(file: File, uri: AmazonS3URI): Unit = {
    val is = new FileInputStream(file)
    try {
      S3Client.DEFAULT.putObject(uri.getBucket, uri.getKey, is, new ObjectMetadata())
    } catch {
      case NonFatal(e) => is.close()
    } finally { is.close() }
  }
}
