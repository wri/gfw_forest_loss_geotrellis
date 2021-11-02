package org.globalforestwatch.util

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.locationtech.jts.geom.Geometry
import geotrellis.vector._
import geotrellis.vector.io.json.JsonFeatureCollection
import _root_.io.circe.generic.auto._
import _root_.io.circe.generic.auto._
import _root_.io.circe.Json
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

object PartitionVisualization {
  case class PartitionInfo(index: Int)
  implicit val enc = ExpressionEncoder[Extent]
  def asGeoJson[A](df: DataFrame): Json = {
    val partitionFeatures =
      df.mapPartitions { partition =>
        if (partition.nonEmpty) {
          val p = partition
            .map { row =>
              val long = row.getDouble(1)
              val lat = row.getDouble(0)
              val extent = Extent(long, lat, long, lat)
              extent
            }
            .reduce(_ combine _)
          Iterator.single(p)
        } else Iterator.empty
      }

    val features = partitionFeatures.collect.map { extent =>
      Feature(extent.toPolygon(), PartitionInfo(0))
    }
    JsonFeatureCollection(features).asJson
  }
}
