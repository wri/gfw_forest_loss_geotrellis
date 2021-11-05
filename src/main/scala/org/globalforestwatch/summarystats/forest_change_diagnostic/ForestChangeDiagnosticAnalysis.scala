 package org.globalforestwatch.summarystats.forest_change_diagnostic

 import cats.data.NonEmptyList
 import cats.data.Validated.{Invalid, Valid, invalid, valid}
 import cats.syntax._

 import java.util
 import scala.collection.JavaConverters._
 import scala.collection.immutable.SortedMap
 import geotrellis.vector.{Feature, Geometry}
 import com.vividsolutions.jts.geom.{Geometry => GeoSparkGeometry}
 import org.apache.spark.rdd.RDD
 import org.apache.spark.sql.SparkSession
 import org.datasyslab.geospark.spatialRDD.SpatialRDD
 import org.globalforestwatch.features.{CombinedFeatureId, FeatureId, FireAlertRDD, GadmFeatureId, GfwProFeature, GfwProFeatureId, GridId, WdpaFeatureId}
 import org.globalforestwatch.grids.GridId.pointGridId
 import org.globalforestwatch.summarystats.{JobError, MultiError, SummaryAnalysis, ValidatedRow}
 import org.globalforestwatch.util.SpatialJoinRDD
 import org.globalforestwatch.util.ImplicitGeometryConverter._
 import org.globalforestwatch.util.Util.getAnyMapValue
 import org.apache.spark.storage.StorageLevel

 object ForestChangeDiagnosticAnalysis extends SummaryAnalysis {

   val name = "forest_change_diagnostic"

   def apply(
     validatedRDD: RDD[(FeatureId, ValidatedRow[Feature[Geometry, FeatureId]])],
     featureType: String,
     intermediateListSource: Option[NonEmptyList[String]],
     fireAlertRDD: SpatialRDD[GeoSparkGeometry],
     kwargs: Map[String, Any]
   )(implicit spark: SparkSession): Unit = {

     val runOutputUrl: String = getOutputUrl(kwargs)

     // These locations can't be processed because of an error in handling their geometry
     val erroredLocationsRDD = validatedRDD.flatMap{ case (fid, feat) => feat.toEither.left.toOption.map { err => (fid, err) } }
     val mainRDD = validatedRDD.flatMap{ case (_, feat) => feat.toEither.right.toOption }

     validatedRDD.persist(StorageLevel.MEMORY_AND_DISK_2)

     // For standard GFW Pro Feature IDs we create a Grid Filter
     // This will allow us to only process those parts of the dissolved list geometry which were updated
     // When using other Feature IDs such as WDPA or GADM,
     // there will be no intermediate results and this part will be ignored
     val gridFilter: List[String] =
       mainRDD
         .filter { feature: Feature[Geometry, FeatureId] =>
           feature.data match {
             case gfwproId: GfwProFeatureId => gfwproId.locationId == -2
             case _ => false
           }
         }
         .map(f => pointGridId(f.geom.getCentroid, 1))
         .collect
           .toList

     val featureRDD: RDD[Feature[Geometry, FeatureId]] =
       toFeatureRdd(mainRDD, gridFilter, intermediateListSource.isDefined)

     featureRDD.persist(StorageLevel.MEMORY_AND_DISK_2)

     val summaryRDD: RDD[(FeatureId, ValidatedRow[ForestChangeDiagnosticSummary])] =
       ForestChangeDiagnosticRDD(
         featureRDD,
         ForestChangeDiagnosticGrid.blockTileGrid,
         kwargs)

     val fireCount: RDD[(FeatureId, ForestChangeDiagnosticDataLossYearly)] =
       ForestChangeDiagnosticAnalysis.fireStats(featureRDD, fireAlertRDD, spark)

     val dataRDD: RDD[(FeatureId, ValidatedRow[ForestChangeDiagnosticData])] = {
       summaryRDD
         .mapValues{ validated => validated.map(_.toForestChangeDiagnosticData().withUpdatedCommodityRisk()) }
         .leftOuterJoin(fireCount)
         .mapValues {
           case (validated, fire) =>
             // Fire results are discarded if joined to error summary
             validated.map { data =>
               data.copy(
                 commodity_threat_fires = fire.getOrElse(ForestChangeDiagnosticDataLossYearly.empty),
                 tree_cover_loss_soy_yearly = data.tree_cover_loss_soy_yearly.limitToMaxYear(2019)
               )
             }
         }
     }

     dataRDD.persist(StorageLevel.MEMORY_AND_DISK_2)

     val finalRDD =
       if (featureType == "gfwpro")
         combineIntermediateList(
           dataRDD,
           gridFilter,
           runOutputUrl,
           spark,
           kwargs)
       else dataRDD

     val rejoinedRDD = finalRDD
       .union(erroredLocationsRDD.map{ case (fid, err) => (fid, invalid[JobError, ForestChangeDiagnosticData](err)) })

     val summaryDF = ForestChangeDiagnosticDF.getFeatureDataFrame(rejoinedRDD, spark)

     ForestChangeDiagnosticExport.export(
       featureType,
       summaryDF,
       runOutputUrl,
       kwargs)

      mainRDD.unpersist()
      featureRDD.unpersist()
      dataRDD.unpersist()
   }

   /**
     * GFW Pro hand of a input features in a TSV file
     * TSV file contains the individual list items, the merged list geometry and the geometric difference from the current merged list geometry and the former one.
     * Individual list items have location IDs >= 0
     * Merged list geometry has location ID -1
     * Geometric difference to previous version has location ID -2
     *
     * Merged list and geometric difference may or may be not present.
     * If geometric difference is present, we only need to process chunks of the merged list which fall into the same grid cells as the geometric difference.
     * Later in the analysis we will then read cached values for the remaining chunks and use them to aggregate list level results.
     * */
   private def toFeatureRdd(
     mainRDD: RDD[Feature[Geometry, FeatureId]],
     gridFilter: List[String],
     useFilter: Boolean
   ): RDD[Feature[Geometry, FeatureId]] = {

     val featureRDD: RDD[Feature[Geometry, FeatureId]] = mainRDD
       .filter { feature: Feature[Geometry, FeatureId] =>
         feature.data match {
           case _: WdpaFeatureId => true
           case _: GadmFeatureId => true
           case gfwproId: GfwProFeatureId if gfwproId.locationId >= 0 => true
           case gfwproId: GfwProFeatureId if gfwproId.locationId == -1 =>
             // If no geometric difference or intermediate result table is present process entire merged list geometry
             if (gridFilter.isEmpty || !useFilter) true
             // Otherwise only process chunks which fall into the same grid cells as the geometric difference
             else gridFilter.contains(pointGridId(feature.geom.getCentroid, 1))
           case _ => false
         }

       }
       .map { feature: Feature[Geometry, FeatureId] =>
         feature.data match {
           case _: WdpaFeatureId => feature
           case _: GadmFeatureId => feature
           case gfwproId: GfwProFeatureId if gfwproId.locationId >= 0 => feature
           case _ =>
             val grid = pointGridId(feature.geom.getCentroid, 1)
             // For merged list, update data to contain the Combine Feature ID including the Grid ID
             Feature(feature.geom, CombinedFeatureId(feature.data, GridId(grid)))
         }
       }

     featureRDD

   }

   def combineIntermediateList(
     dataRDD: RDD[(FeatureId, ValidatedRow[ForestChangeDiagnosticData])],
     gridFilter: List[String],
     outputUrl: String,
     spark: SparkSession,
     kwargs: Map[String, Any]
   ): RDD[(FeatureId, ValidatedRow[ForestChangeDiagnosticData])] = {

     val intermediateListSource = getAnyMapValue[Option[NonEmptyList[String]]](
       kwargs,
       "intermediateListSource"
     )

     // Get merged list RDD
     val listRDD = dataRDD.filter { _._1 match {
       case _: CombinedFeatureId => true
       case _ => false
     }}

     // Get row RDD
     val rowRDD = dataRDD.filter { _._1 match {
       case _: CombinedFeatureId => false
       case _ => true
     }}

     // combine filtered List with filtered intermediate results
     val combinedListRDD = {
       if (intermediateListSource.isDefined) {
         ForestChangeDiagnosticDF.readIntermidateRDD(intermediateListSource.get, spark)
           .filter { _._1 match {
             case combinedId: CombinedFeatureId =>
               !gridFilter.contains(combinedId.featureId2.toString)
             case _ => false
           }}
           .union(listRDD)
       } else listRDD
     }

     // EXPORT new intermediate results
     val combinedListDF = ForestChangeDiagnosticDF.getGridFeatureDataFrame(combinedListRDD, spark)

     ForestChangeDiagnosticExport.export(
       "intermediate",
       combinedListDF,
       outputUrl,
       kwargs
     )

     // Reduce by feature ID and update commodity risk
     val updatedListRDD = combinedListRDD
       .map {
         case (id, data) =>
           id match {
             case combinedId: CombinedFeatureId =>
               (combinedId.featureId1, data)
           }
       }
       .reduceByKey(_ combine _)
       .mapValues { _.map(_.withUpdatedCommodityRisk()) }

     // Merge with row RDD
     rowRDD.union(updatedListRDD)
   }

   def fireStats(
     featureRDD: RDD[Feature[Geometry, FeatureId]],
     fireAlertRDD: SpatialRDD[GeoSparkGeometry],
     spark: SparkSession
   ): RDD[(FeatureId, ForestChangeDiagnosticDataLossYearly)] = {
     // Convert FeatureRDD to SpatialRDD
     val polyRDD = featureRDD.map { feature =>
       // Implicitly convert to GeoSparkGeometry
       val geom: GeoSparkGeometry = feature.geom
       geom.setUserData(feature.data)
       geom
     }
     val spatialFeatureRDD = new SpatialRDD[GeoSparkGeometry]
     spatialFeatureRDD.rawSpatialRDD = polyRDD.toJavaRDD()
     spatialFeatureRDD.fieldNames = seqAsJavaList(List("FeatureId"))
     spatialFeatureRDD.analyze()

     if (spatialFeatureRDD.boundary != null) {
       val joinedRDD =
         SpatialJoinRDD.spatialjoin(
           spatialFeatureRDD,
           fireAlertRDD,
           usingIndex = true
         )

       joinedRDD.rdd
         .map {
           case (poly, points) =>
             toForestChangeDiagnosticFireData(poly, points)
         }
         .reduceByKey(_ merge _)
         .mapValues { fires =>
           aggregateFireData(fires).limitToMaxYear(2019)
         }
     } else {
       spark.sparkContext.emptyRDD
     }
   }

   private def toForestChangeDiagnosticFireData(
                                                 poly: GeoSparkGeometry,
                                                 points: util.HashSet[GeoSparkGeometry]
                                               ): (FeatureId, ForestChangeDiagnosticDataLossYearly) = {
     (poly.getUserData.asInstanceOf[FeatureId], {
       val fireCount =
         points.asScala.toList.foldLeft(SortedMap[Int, Double]()) {
           (z: SortedMap[Int, Double], point) => {
             // extract year from acq_date column
             val year = point.getUserData
               .asInstanceOf[String]
               .split("\t")(2)
               .substring(0, 4)
               .toInt
             val count = z.getOrElse(year, 0.0) + 1.0
             z.updated(year, count)
           }
         }

       ForestChangeDiagnosticDataLossYearly.prefilled
         .merge(ForestChangeDiagnosticDataLossYearly(fireCount))
     })
   }

   private def aggregateFireData(
                                  fires: ForestChangeDiagnosticDataLossYearly
                                ): ForestChangeDiagnosticDataLossYearly = {
     val minFireYear = fires.value.keysIterator.min
     val maxFireYear = fires.value.keysIterator.max
     val years: List[Int] = List.range(minFireYear + 1, maxFireYear + 1)

     ForestChangeDiagnosticDataLossYearly(
       SortedMap(
         years.map(
           year =>
             (year, {
               val thisYearFireCount: Double = fires.value.getOrElse(year, 0)
               val lastYearFireCount: Double = fires.value.getOrElse(year - 1, 0)
               (thisYearFireCount + lastYearFireCount) / 2
             })
         ): _*
       )
     )
   }
 }
