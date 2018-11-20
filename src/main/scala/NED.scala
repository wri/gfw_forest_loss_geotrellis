package geotrellis.demo.footprint

import com.amazonaws.services.s3.AmazonS3URI
import geotrellis.contrib.vlm.RasterSource
import geotrellis.contrib.vlm.gdal.GDALRasterSource
import geotrellis.raster.{CellSize, RasterExtent}
import geotrellis.spark.io.s3.S3Client
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.Extent
import java.nio.file.{Paths, Files}
import scala.collection.JavaConverters._

object NED {
  /** Tile over NAD83 at NED cell size */
  val layoutDefinition: LayoutDefinition = {
    val nedRasterExtent = RasterExtent(
      Extent(-180.0000, -90.0000, 180.0000, 90.0000),
      CellSize(0.000030864197531, 0.000030864197531))
    LayoutDefinition(nedRasterExtent, 1024, 1024)
  }

  /** List all *.img files in a given uri (s3:, file:) supported */
  def getRasters(uri: String): Seq[RasterSource] = {
    val VsiS3Rx = """/vsis3/(\w+)/(.*)""".r
    uri match {
      case VsiS3Rx(bucket, prefix) =>
        S3Client.DEFAULT.listKeys(bucket, prefix)
          .filter(_.endsWith(".img"))
          .map { prefix =>
            val path = s"/vsis3/$bucket/$prefix"
            println(s"Reading: $path")
            GDALRasterSource(path)
          }

      case _ =>
        Files.walk(Paths.get(uri), 4)
          .iterator().asScala
          .filter(_.toString.endsWith(".img"))
          .map { path =>
            println(s"Reading: $path")
            GDALRasterSource(path.toString)}
          .toSeq
    }
  }
}
