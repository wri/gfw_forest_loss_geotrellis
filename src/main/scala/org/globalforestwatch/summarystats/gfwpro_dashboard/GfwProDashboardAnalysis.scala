package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.data.{NonEmptyList, Validated}
import geotrellis.vector.{Feature, Geometry}
import org.globalforestwatch.features._
import org.globalforestwatch.summarystats._
import org.globalforestwatch.util.GeometryConstructor.createPoint
import org.globalforestwatch.util.{RDDAdapter, SpatialJoinRDD}
import org.globalforestwatch.ValidatedWorkflow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.globalforestwatch.features.FeatureId
import org.apache.sedona.sql.utils.Adapter
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.globalforestwatch.util.GeotrellisGeometryValidator.makeValidGeom
import org.globalforestwatch.util.Util

import scala.collection.JavaConverters._
import java.time.LocalDate
import org.globalforestwatch.util.IntersectGeometry
import cats.data.Validated.{Invalid, Valid}

object GfwProDashboardAnalysis extends SummaryAnalysis {

  val name = "gfwpro_dashboard"

  /** Run the GFWPro dashboard analysis.
    *
    * If doGadmIntersect is true, read in the entire gadm feature dataset and
    * intersect with the user feature list to determine the relevant gadm areas. If
    * doGadmIntersect is false (usually for small number of user features), then
    * determine the relevant gadm areas by using the raster gadm datasets GadmAdm0,
    * GadmAdm1, and GadmAdm2.
    */
  def apply(
    featureRDD: RDD[ValidatedLocation[Geometry]],
    featureType: String,
    doGadmIntersect: Boolean,
    gadmFeatureUrl: NonEmptyList[String],
    fireAlertRDD: SpatialRDD[Geometry],
    spark: SparkSession,
    kwargs: Map[String, Any]
  ): ValidatedWorkflow[Location[JobError],(FeatureId, GfwProDashboardData)] = {
    featureRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val summaryRDD = ValidatedWorkflow(featureRDD).flatMap { rdd =>
      val enrichedRDD = if (doGadmIntersect) {
        println("Doing intersect with vector gadm")
        val spatialContextualDF = SpatialFeatureDF(gadmFeatureUrl, "gadm", FeatureFilter.empty, "geom", spark)
        val spatialContextualRDD = Adapter.toSpatialRdd(spatialContextualDF, "polyshape")
        // For features that have been split into multiple rows, remove the centroid
        // from all but the one whose geometry contains the actual centroid. This will
        // ensure we get the one gadmid which contains the centroid of the feature's
        // whole geometry.
        val adjustedRdd = rdd.map({
          case loc@Location(CombinedFeatureId(fid, centroid:PointFeatureId), geom) =>
            if (geom.contains(centroid.pt)) {
              loc
            } else {
              Location(CombinedFeatureId(fid, PointFeatureId(createPoint(0, 0))), geom)
            }
          case loc => loc
        })
        val spatialFeatureRDD = RDDAdapter.toSpatialRDDfromLocationRdd(adjustedRdd, spark)

        /* Enrich the feature RDD by intersecting it with contextual features
         * The resulting FeatureId carries combined identity of source feature and contextual geometry
         */
        SpatialJoinRDD
          .flatSpatialJoin(spatialContextualRDD, spatialFeatureRDD, considerBoundaryIntersection = true, usingIndex = true)
          .rdd
          .flatMap { case (feature, context) =>
            refineContextualIntersection(feature, context, "gadm")
          }
      } else {
        println("Using raster gadm")
        rdd.map {
          case Location(CombinedFeatureId(id@GfwProFeatureId(listId, locationId), featureCentroid: PointFeatureId), geom) => {
            if (locationId != -1) {
              // For a non-dissolved location, determine the GadmFeatureId for the
              // centroid of the location's geometry, and add that to the feature id.
              // This can be expensive, since the tile reads are not cached. So, we
              // we only use this raster GADM approach for user inputs with a small
              // number of locations (e.g. <50). In that case, we get significant
              // performance improvement by not having to read in the entire vector
              // GADM file, but instead only reading the GADM raster tiles for the
              // relevant areas.
              val pt = featureCentroid.pt
              val windowLayout = GfwProDashboardGrid.blockTileGrid
              val key = windowLayout.mapTransform.keysForGeometry(pt).toList.head
              val rasterSource = GfwProDashboardRDD.getSources(key, windowLayout, kwargs).getOrElse(null)
              val raster = rasterSource.readWindow(key, windowLayout).getOrElse(null)
              val re = raster.rasterExtent
              val col = re.mapXToGrid(pt.getX())
              val row = re.mapYToGrid(pt.getY())
              Validated.valid[Location[JobError], Location[Geometry]](Location(CombinedFeatureId(id, GadmFeatureId(raster.tile.gadm0.getData(col, row),
                raster.tile.gadm1.getData(col, row),
                raster.tile.gadm2.getData(col, row))), geom))
            } else {
              // For a dissolved location, add a dummy GadmFeatureId to the feature id.
              Validated.valid[Location[JobError], Location[Geometry]](Location(CombinedFeatureId(id, GadmFeatureId("X", 0, 0)), geom))
            }
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
          // This is where the main analysis happens, including calling
          // GfwProDashboardSummary.getGridVisitor.visit on each pixel.
          val validatedSummaryStatsRdd = GfwProDashboardRDD(tmp,
               GfwProDashboardGrid.blockTileGrid,
               kwargs + ("getRasterGadm" -> !doGadmIntersect))

          // If a location has no GfwProDashboardRawDataGroup entries, then the
          // geometry must not have intersected the centroid of any pixels, so report
          // the location as NoIntersectionError.
          val validatedSummaryStatsRdd1 = validatedSummaryStatsRdd.map {
            case Valid(Location(fid, data)) if data.isEmpty =>
              Invalid(Location(fid, NoIntersectionError))
            case data => data
          }

          ValidatedWorkflow(validatedSummaryStatsRdd1).mapValid { summaryStatsRDD =>
            summaryStatsRDD
              .flatMap { case (CombinedFeatureId(fid@GfwProFeatureId(listId, locationId), gadmId), summary) =>
                // For non-dissolved locations or vector gadm intersection, merge all
                // summaries (ignoring any differing group_gadm_id), and move the
                // gadmId from the featureId into the group_gadm_id. For dissolved
                // locations for raster gadm, merge summaries into multiple rows
                // based on the per-pixel group_gadm_id.
                val ignoreRasterGadm = locationId != -1 || doGadmIntersect
                summary.toGfwProDashboardData(ignoreRasterGadm).map( x => {
                  val newx = if (ignoreRasterGadm) {
                    val gadmFeatureId = gadmId.asInstanceOf[GadmFeatureId]
                    val gadmString = if (gadmFeatureId != null) {
                      Util.getGadmId(gadmFeatureId.iso, gadmFeatureId.adm1, gadmFeatureId.adm2, kwargs("gadmVers").asInstanceOf[String])
                    } else {
                      Util.getGadmId("", null, null, kwargs("gadmVers").asInstanceOf[String])
                    }
                    x.copy(group_gadm_id = gadmString)
                  } else {
                    x
                  }
                  Location(fid, newx)
                }
                )
                case _ => throw new NotImplementedError("Missing case")
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

  /** These geometries touch, apply application specific logic of how to treat that.
    *   - For intersection of location geometries, only keep those where centroid of
    *     location is in the contextual geom (this ensures that any location is only
    *     assigned to a single contextual area even if it intersects more).
    *   - For dissolved geometry of list, report all contextual areas it intersects
    */
  private def refineContextualIntersection(
    featureGeom: Geometry,
    contextualGeom: Geometry,
    contextualFeatureType: String
  ): List[ValidatedLocation[Geometry]] = {
    val featureId = featureGeom.getUserData.asInstanceOf[FeatureId]
    val contextualId = FeatureId.fromUserData(contextualFeatureType, contextualGeom.getUserData.asInstanceOf[String], delimiter = ",")

    featureId match {
      // ValidateFeatureRDD already computed the centroid of the location (when
      // locationId != -1) and stashed it in the FeatureId.
      case CombinedFeatureId(gfwproId: GfwProFeatureId, mainCentroid: PointFeatureId) if gfwproId.locationId >= 0 =>
        val hasMainCentroid = (mainCentroid.pt.getX() != 0 && mainCentroid.pt.getY() != 0)
        val featureCentroid =
          if (hasMainCentroid) {
            null
          } else {
            val centroid = featureGeom.getCentroid
            createPoint(centroid.getX, centroid.getY)
          }
        val fixedGeom = makeValidGeom(featureGeom)
        if (hasMainCentroid && contextualGeom.contains(mainCentroid.pt)) {
          val fid = CombinedFeatureId(gfwproId, contextualId)
          List(Validated.Valid(Location(fid, fixedGeom)))
        } else if (!hasMainCentroid && contextualGeom.contains(featureCentroid)) {
          val fid = CombinedFeatureId(gfwproId, null)
          List(Validated.Valid(Location(fid, fixedGeom)))
        } else {
          Nil
        }

      case CombinedFeatureId(gfwproId: GfwProFeatureId, _) if gfwproId.locationId < 0 =>
        IntersectGeometry
          .validatedIntersection(featureGeom, contextualGeom)
          .leftMap { err => Location(featureId, err) }
          .map { geometries =>
            geometries.map { geom =>
              // val gtGeom: Geometry = toGeotrellisGeometry(geom)
              val fixedGeom = makeValidGeom(geom)
              Location(CombinedFeatureId(gfwproId, contextualId), fixedGeom)
            }
          }
          .traverse(identity) // turn validated list of geometries into list of validated geometries
    }
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
