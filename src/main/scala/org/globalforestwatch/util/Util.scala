package org.globalforestwatch.util

import java.io.{File, FileInputStream}

import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.ObjectMetadata
import geotrellis.raster.RasterExtent
import geotrellis.spark.io.index.zcurve.Z2
import geotrellis.spark.io.s3.S3Client
import geotrellis.vector.{Feature, Geometry, Point, Polygon}

import scala.util.control.NonFatal

object Util {
  def uploadFile(file: File, uri: AmazonS3URI): Unit = {
    val is = new FileInputStream(file)
    try {
      S3Client.DEFAULT.putObject(uri.getBucket, uri.getKey, is, new ObjectMetadata())
    } catch {
      case NonFatal(e) => is.close()
    } finally { is.close() }
  }

  def sortByZIndex[G <: Geometry, A](
    features: Seq[Feature[G, A]],
    rasterExtent: RasterExtent
  ): Seq[Feature[G, A]] = {
    def zindex(p: Point): Long = {
      val col = rasterExtent.mapXToGrid(p.x)
      val row = rasterExtent.mapXToGrid(p.y)
      Z2(col, row).z
    }

    features.sortBy{ feature =>
      zindex(feature.geom.envelope.northWest)
    }
  }
}
