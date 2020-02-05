package org.globalforestwatch.summarystats.firealerts

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.{Feature, Geometry, Point}
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.globalforestwatch.features.{FeatureId, FireAlertFeatureId}
import org.globalforestwatch.summarystats.firealerts.FireAlertsRDD.SUMMARY
import org.globalforestwatch.util.Util._

import scala.reflect.ClassTag

object FireAlertsAnalysis {
  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            part: HashPartitioner,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    import spark.implicits._

    val fireAlertFeatureRDD = getFireAlertFeatureRDD(featureRDD, FireAlertsGrid.blockTileGrid, part, spark, kwargs)

    val summaryRDD: RDD[(FireAlertFeatureId, FireAlertsSummary)] =
      FireAlertsRDD(fireAlertFeatureRDD, FireAlertsGrid.blockTileGrid, part, kwargs)

    val summaryDF =
      FireAlertsDFFactory(featureType, summaryRDD, spark).getDataFrame

    summaryDF.repartition(partitionExprs = $"id")

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      "/fireAlerts_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    FireAlertsExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
  }


  def getFireAlertFeatureRDD(featureRDD: RDD[Feature[Geometry, FeatureId]],
                      windowLayout: LayoutDefinition,
                      partitioner: Partitioner,
                      spark: SparkSession,
                      kwargs: Map[String, Any])(implicit kt: ClassTag[SUMMARY], vt: ClassTag[FeatureId], ord: Ordering[SUMMARY] = null):  RDD[Feature[Geometry, FireAlertFeatureId]] = {

    val fireScientificUri = if (kwargs.get("fireAlertSource").equals("modis")) "file:/Users/justin.terry/dev/gfw_forest_loss_geotrellis/input/fire_alerts_modis.tsv" else "file:/Users/justin.terry/dev/gfw_forest_loss_geotrellis/input/fire_alerts_viirs.tsv"

    val windowLayout = FireAlertsGrid.blockTileGrid // TODO should use viirs/modis grid
    val pointRDD = spark
      .read
      .options(Map("header" -> "true", "delimiter" -> "\t"))
      .csv(fireScientificUri)
      .rdd.mapPartitions({iter: Iterator[Row] => {
      for {
        i <- iter
      } yield {
        val lat: Float = i.getString(0).toFloat
        val lon: Float = i.getString(1).toFloat
        val alertDate: String = i.getString(2)
        val geom: Geometry = Point(lon, lat)

        geotrellis.vector
          .Feature(geom, (lat, lon, alertDate))
      }
    }}, preservesPartitioning = true)
      .flatMap { feature =>
        val keys: Set[SpatialKey] =
          windowLayout.mapTransform.keysForGeometry(feature.geom)
        keys.toSeq.map { key =>
          (key, feature)
        }
      }
      .partitionBy(partitioner)

    val polygonRDD = featureRDD.flatMap { feature: Feature[Geometry, FeatureId] =>
      val keys: Set[SpatialKey] =
        windowLayout.mapTransform.keysForGeometry(feature.geom)
      keys.toSeq.map { key =>
        (key, feature)
      }
    }
      .partitionBy(partitioner)

    val pipRDD: RDD[Feature[Geometry, FireAlertFeatureId]] = pointRDD
      .cogroup(polygonRDD)
      .mapPartitions(iter => {
        //val preparedGeometryFactory = new PreparedGeometryFactory()
        iter.flatMap {
          case (_, (points, polygons)) => {
            val polygonArr = polygons.toArray
            points.flatMap(point => {
              polygonArr
                .filter(_.intersects(point))
                .map(polygon => {
                  Feature(point.geom, FireAlertFeatureId(point.data._3, polygon.data)) // TODO should be more clear than _3
                })
            })
          }
        }
      })

    pipRDD
  }
}
