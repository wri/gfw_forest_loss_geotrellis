package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.data.{NonEmptyList, Validated}
import geotrellis.vector.{Feature, Geometry}
import org.globalforestwatch.features._
import org.globalforestwatch.summarystats._
import org.globalforestwatch.util.GeometryConstructor.createPoint
import org.globalforestwatch.util.{RDDAdapter, SpatialJoinRDD}
import org.globalforestwatch.util.RDDAdapter
import org.globalforestwatch.ValidatedWorkflow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.globalforestwatch.features.FeatureId
import org.apache.sedona.core.spatialRDD.SpatialRDD

import scala.collection.JavaConverters._
import java.time.LocalDate

object GfwProDashboardAnalysis extends SummaryAnalysis {

  val name = "gfwpro_dashboard"

  def apply(
    featureRDD: RDD[ValidatedLocation[Geometry]],
    featureType: String,
    contextualFeatureType: String,
    contextualFeatureUrl: NonEmptyList[String],
    fireAlertRDD: SpatialRDD[Geometry],
    spark: SparkSession,
    kwargs: Map[String, Any]
  ): ValidatedWorkflow[Location[JobError],(FeatureId, GfwProDashboardData)] = {
    featureRDD.persist(StorageLevel.MEMORY_AND_DISK)

    println(s"XXX ${featureRDD.getNumPartitions}")
    featureRDD.glom().map(_.size).collect().foreach(println)
    val xxRDD = featureRDD.repartition(spark.sparkContext.defaultParallelism)
    println(s"YYY ${xxRDD.getNumPartitions}")
    xxRDD.glom().map(_.size).collect().foreach(println)
    val summaryRDD = ValidatedWorkflow(xxRDD).flatMap { rdd =>
      val enrichedRDD = rdd.map {
        case Location(id@GfwProFeatureId(listId, locationId), geom) => {
          if (locationId != -1) {
            // For a non-dissolved location, determine the GadmFeatureId for the
            // centroid of the location's geometry, and add that to the feature id.
            val pt = createPoint(geom.getCentroid.getX, geom.getCentroid.getY)
            val windowLayout = GfwProDashboardGrid.blockTileGrid
            val key = windowLayout.mapTransform.keysForGeometry(pt).toList.head
            val rasterSource = GfwProDashboardRDD.getSources(key, windowLayout, kwargs).getOrElse(null)
            val raster = rasterSource.readWindow(key, windowLayout).getOrElse(null)
            val re = raster.rasterExtent
            val col = re.mapXToGrid(pt.getX())
            val row = re.mapYToGrid(pt.getY())
            //println(s"YYY $id $pt $key $raster $geom")
            //println(s"ZZZ ${raster.tile.gadm0.getData(col, row)}.${raster.tile.gadm1.getData(col, row)}.${raster.tile.gadm2.getData(col, row)}")
            Validated.valid[Location[JobError], Location[Geometry]](Location(CombinedFeatureId(id, GadmFeatureId(raster.tile.gadm0.getData(col, row),
              raster.tile.gadm1.getData(col, row),
              raster.tile.gadm2.getData(col, row))), geom))
          } else {
            // For a dissolved location, add a dummy GadmFeatureId to the feature id.
            Validated.valid[Location[JobError], Location[Geometry]](Location(CombinedFeatureId(id, GadmFeatureId("X", 0, 0)), geom))
          }
        }
      }

      ValidatedWorkflow(enrichedRDD)
        .mapValidToValidated { rdd =>
          rdd.map { case row@Location(fid, geom) =>
            if (geom.isEmpty()) {
              Validated.invalid[Location[JobError], Location[Geometry]](Location(fid, GeometryError(s"Empty Geometry")))
            } else if (!geom.isValid) {
              Validated.invalid[Location[JobError], Location[Geometry]](Location(fid, GeometryError(s"Invalid Geometry")))
            } else
              Validated.valid[Location[JobError], Location[Geometry]](row)
          }
        }
        .flatMap { enrichedRDD =>
          val fireStatsRDD = fireStats(enrichedRDD, fireAlertRDD, spark)
          val tmp = enrichedRDD.map { case Location(id, geom) => Feature(geom, id) }
          val validatedSummaryStatsRdd = GfwProDashboardRDD(tmp, GfwProDashboardGrid.blockTileGrid, kwargs)
          ValidatedWorkflow(validatedSummaryStatsRdd).mapValid { summaryStatsRDD =>
            summaryStatsRDD
              //.flatMapValues(_.toGfwProDashboardData())
              //.flatMap { case (fid, summary) => summary.toGfwProDashboardData(true).map( x => (fid, x)) }
              // 
              .flatMap { case (CombinedFeatureId(fid@GfwProFeatureId(listId, locationId), gadmId), summary) =>
                // For non-dissolved locations, merge all summaries ignoring any
                // differing gadmId, and move the gadmId from the centroid into the
                // group_gadm_id. For dissolved locations, merge summaries into multiple
                // rows based on the group (per-pixel) gadmId.
                summary.toGfwProDashboardData(locationId != -1).map( x => {
                  val newx = if (locationId == -1) {
                    x
                  } else {
                    x.copy(group_gadm_id = gadmId.toString)
                  }
                  Location(fid, newx)
                }
                )
              }
              // fold in fireStatsRDD after polygonal summary and accumulate the errors
              .leftOuterJoin(fireStatsRDD)
              .mapValues { case (data, fire) =>
                data.copy(viirs_alerts_daily = fire.getOrElse(GfwProDashboardDataDateCount.empty))
              }
          }
        }
    }
    summaryRDD

  }

  private def fireStats(
    featureRDD: RDD[Location[Geometry]],
    fireAlertRDD: SpatialRDD[Geometry],
    spark: SparkSession
  ): RDD[Location[GfwProDashboardDataDateCount]] = {
    val featureSpatialRDD = RDDAdapter.toSpatialRDDfromLocationRdd(featureRDD, spark)
    // If there are no locations that intersect the TCL extent (featureSpatialRDD is
    // empty, has no envelope), then spatial join below will fail, so return without
    // further analysis.
    if (featureSpatialRDD.boundaryEnvelope == null) {
      return spark.sparkContext.parallelize(Seq.empty[Location[GfwProDashboardDataDateCount]])
    }
    val joinedRDD = SpatialJoinRDD.spatialjoin(featureSpatialRDD, fireAlertRDD)

    joinedRDD.rdd
      .map { case (poly, points) =>
        val fid = poly.getUserData.asInstanceOf[FeatureId]
        val data = points.asScala.foldLeft(GfwProDashboardDataDateCount.empty) { (z, point) =>
          // extract year from acq_date column is YYYY-MM-DD
          val acqDate = point.getUserData.asInstanceOf[String].split("\t")(2)
          val alertDate = LocalDate.parse(acqDate)
          z.merge(GfwProDashboardDataDateCount.fillDaily(Some(alertDate), true, 1))
        }
        (fid, data)
      }
      .reduceByKey(_ merge _)
  }
}
