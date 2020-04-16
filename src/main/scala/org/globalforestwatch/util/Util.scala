package org.globalforestwatch.util

import java.io.{File, FileInputStream}

import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.ObjectMetadata
import geotrellis.raster.RasterExtent
import geotrellis.spark.SpatialKey
import geotrellis.spark.io.index.zcurve.Z2
import geotrellis.spark.io.s3.S3Client
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.{Feature, Geometry, Point, Polygon}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.globalforestwatch.features.FeatureId

import scala.util.control.NonFatal

object Util {
  def uploadFile(file: File, uri: AmazonS3URI): Unit = {
    val is = new FileInputStream(file)
    try {
      S3Client.DEFAULT.putObject(
        uri.getBucket,
        uri.getKey,
        is,
        new ObjectMetadata()
      )
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

    features.sortBy { feature =>
      zindex(feature.geom.envelope.northWest)
    }
  }

  def getKeyedFeatureRDD[FEATUREID <: FeatureId](featureRDD: RDD[Feature[Geometry, FEATUREID]],
                                                 windowLayout: LayoutDefinition,
                                                 partitioner: Partitioner) = {
    featureRDD
      .flatMap { feature: Feature[Geometry, FEATUREID] =>
        val keys: Set[SpatialKey] =
          windowLayout.mapTransform.keysForGeometry(feature.geom)
        keys.toSeq.map { key =>
          (key, feature)
        }
      }
      .partitionBy(partitioner)
  }

  def getAnyMapValue[T: Manifest](map: Map[String, Any], key: String): T =
    map(key) match {
      case v: T => v
      case _ => throw new IllegalArgumentException("Wrong type")
    }
}
