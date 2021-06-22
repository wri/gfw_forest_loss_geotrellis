package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.data.NonEmptyList

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util
import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap
import geotrellis.vector.{Feature, Geometry}
import com.vividsolutions.jts.geom.{Geometry => GeoSparkGeometry}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.globalforestwatch.features.{CombinedFeatureId, FeatureDF, FeatureId, FireAlertRDD, GfwProFeature, GfwProFeatureId, GridId}
import org.globalforestwatch.grids.GridId.pointGridId
import org.globalforestwatch.util.SpatialJoinRDD
import org.globalforestwatch.util.ImplicitGeometryConverter._
import org.globalforestwatch.util.Util.{getAnyMapValue, sortByZIndex}

//import org.apache.sedona.core.enums.{FileDataSplitter, GridType, IndexType}
//import org.apache.sedona.core.spatialOperator.JoinQuery
//import org.apache.sedona.core.spatialRDD.PointRDD
//import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}

object ForestChangeDiagnosticAnalysis {

  val logger: Logger = Logger.getLogger("ForestChangeDiagnosticAnalysis")

  def apply(mainRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    val intermediateListSource = getAnyMapValue[Option[NonEmptyList[String]]](
      kwargs,
      "intermediateListSource"
    )
    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      "/forest_change_diagnostic_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    mainRDD.cache()

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

    mainRDD.unpersist()
    featureRDD.cache()

    val summaryRDD: RDD[(FeatureId, ForestChangeDiagnosticSummary)] =
      ForestChangeDiagnosticRDD(
        featureRDD,
        ForestChangeDiagnosticGrid.blockTileGrid,
        kwargs
      )

    val fireCount: RDD[(FeatureId, ForestChangeDiagnosticDataLossYearly)] =
      ForestChangeDiagnosticAnalysis.fireStats(featureRDD, spark, kwargs)

    featureRDD.unpersist()

    val dataRDD: RDD[(FeatureId, ForestChangeDiagnosticData)] =
      reformatSummaryData(summaryRDD)
        .reduceByKey(_ merge _)
        .map { case (id, data) => updateCommodityRisk(id, data) }
        .leftOuterJoin(fireCount)
        .mapValues {
          case (data, fire) =>
            data.update(
              fireThreatIndicator =
                fire.getOrElse(ForestChangeDiagnosticDataLossYearly.empty)
            )
        }

    dataRDD.cache()

    val finalRDD =
      combineIntermediateList(dataRDD, gridFilter, runOutputUrl, spark, kwargs)

    val summaryDF =
      ForestChangeDiagnosticDFFactory(featureType, finalRDD, spark, kwargs).getDataFrame

    ForestChangeDiagnosticExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
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
                               dataRDD: RDD[(FeatureId, ForestChangeDiagnosticData)],
                               gridFilter: List[String],
                               outputUrl: String,
                               spark: SparkSession,
                               kwargs: Map[String, Any]
                             ): RDD[(FeatureId, ForestChangeDiagnosticData)] = {

    val intermediateListSource = getAnyMapValue[Option[NonEmptyList[String]]](
      kwargs,
      "intermediateListSource"
    )

    // Get merged list RDD
    val listRDD: RDD[(FeatureId, ForestChangeDiagnosticData)] = {
      dataRDD.filter { data: (FeatureId, ForestChangeDiagnosticData) =>
        data._1 match {
          case _: CombinedFeatureId => true
          case _ => false
        }
      }
    }

    // Get row RDD
    val rowRDD = dataRDD.filter {
      data: (FeatureId, ForestChangeDiagnosticData) =>
        data._1 match {
          case _: CombinedFeatureId => false
          case _ => true
        }
    }

    // combine filtered List with filtered intermediate results
    val combinedListRDD = {
      if (intermediateListSource.isDefined) {
        val intermediateRDD: RDD[(FeatureId, ForestChangeDiagnosticData)] =
          getIntermediateRDD(intermediateListSource.get, spark, kwargs)

        listRDD ++
          intermediateRDD.filter {
            data: (FeatureId, ForestChangeDiagnosticData) =>
              data._1 match {
                case combinedId: CombinedFeatureId =>
                  !gridFilter.contains(combinedId.featureId2.toString)
                case _ => false
              }
          }
      } else listRDD
    }

    // EXPORT new intermediate results
    val combinedListDF = ForestChangeDiagnosticDFFactory(
      "grid",
      combinedListRDD,
      spark,
      kwargs
    ).getDataFrame

    ForestChangeDiagnosticExport.exportIntermediateList(
      combinedListDF,
      outputUrl
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
      .reduceByKey(_ merge _)
      .map { case (id, data) => updateCommodityRisk(id, data) }

    // Merge with row RDD
    rowRDD ++ updatedListRDD

  }

  def fireStats(
                 featureRDD: RDD[Feature[Geometry, FeatureId]],
                 spark: SparkSession,
                 kwargs: Map[String, Any]
               ): RDD[(FeatureId, ForestChangeDiagnosticDataLossYearly)] = {

    // FIRE RDD
    val fireAlertSpatialRDD = FireAlertRDD(spark, kwargs)

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

    val joinedRDD =
      SpatialJoinRDD.spatialjoin(spatialFeatureRDD, fireAlertSpatialRDD, usingIndex = true)

    joinedRDD.rdd
      .map {
        case (poly, points) =>
          toForestChangeDiagnosticFireData(poly, points)
      }
      .reduceByKey(_ merge _)
      .mapValues { fires =>
        aggregateFireData(fires)
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

  private def reformatSummaryData(
                                   summaryRDD: RDD[(FeatureId, ForestChangeDiagnosticSummary)]
                                 ): RDD[(FeatureId, ForestChangeDiagnosticData)] = {

    summaryRDD
      .flatMap {
        case (id, summary) =>
          // We need to convert the Map to a List in order to correctly flatmap the data
          summary.stats.toList.map {
            case (dataGroup, data) =>
              //              id match {
              //                case featureId: SimpleFeatureId =>
              toForestChangeDiagnosticData(id, dataGroup, data)
            //                case _ =>
            //                  throw new IllegalArgumentException("Not a SimpleFeatureId")
            //              }
          }
      }

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

  private def toForestChangeDiagnosticData(
                                            featureId: FeatureId,
                                            dataGroup: ForestChangeDiagnosticRawDataGroup,
                                            data: ForestChangeDiagnosticRawData
                                          ): (FeatureId, ForestChangeDiagnosticData) = {
    (
      featureId,
      ForestChangeDiagnosticData(
        treeCoverLossTcd30Yearly = ForestChangeDiagnosticDataLossYearly.fill(
          dataGroup.umdTreeCoverLossYear,
          data.totalArea,
          dataGroup.isUMDLoss
        ),
        treeCoverLossTcd90Yearly = ForestChangeDiagnosticDataLossYearly.fill(
          dataGroup.umdTreeCoverLossYear,
          data.totalArea,
          dataGroup.isUMDLoss && dataGroup.isTreeCoverExtent90
        ),
        treeCoverLossPrimaryForestYearly =
          ForestChangeDiagnosticDataLossYearly.fill(
            dataGroup.umdTreeCoverLossYear,
            data.totalArea,
            dataGroup.isPrimaryForest && dataGroup.isUMDLoss
          ),
        treeCoverLossPeatLandYearly = ForestChangeDiagnosticDataLossYearly.fill(
          dataGroup.umdTreeCoverLossYear,
          data.totalArea,
          dataGroup.isPeatlands && dataGroup.isUMDLoss
        ),
        treeCoverLossIntactForestYearly =
          ForestChangeDiagnosticDataLossYearly.fill(
            dataGroup.umdTreeCoverLossYear,
            data.totalArea,
            dataGroup.isIntactForestLandscapes2000 && dataGroup.isUMDLoss
          ),
        treeCoverLossProtectedAreasYearly =
          ForestChangeDiagnosticDataLossYearly.fill(
            dataGroup.umdTreeCoverLossYear,
            data.totalArea,
            dataGroup.isProtectedArea && dataGroup.isUMDLoss
          ),
        treeCoverLossSEAsiaLandCoverYearly =
          ForestChangeDiagnosticDataLossYearlyCategory.fill(
            dataGroup.seAsiaLandCover,
            dataGroup.umdTreeCoverLossYear,
            data.totalArea,
            include = dataGroup.isUMDLoss
          ),
        treeCoverLossIDNLandCoverYearly =
          ForestChangeDiagnosticDataLossYearlyCategory.fill(
            dataGroup.idnLandCover,
            dataGroup.umdTreeCoverLossYear,
            data.totalArea,
            include = dataGroup.isUMDLoss
          ),
        treeCoverLossSoyPlanedAreasYearly =
          ForestChangeDiagnosticDataLossYearly.fill(
            dataGroup.umdTreeCoverLossYear,
            data.totalArea,
            dataGroup.isSoyPlantedAreas && dataGroup.isUMDLoss
          ),
        treeCoverLossIDNForestAreaYearly =
          ForestChangeDiagnosticDataLossYearlyCategory.fill(
            dataGroup.idnForestArea,
            dataGroup.umdTreeCoverLossYear,
            data.totalArea,
            include = dataGroup.isUMDLoss
          ),
        treeCoverLossIDNForestMoratoriumYearly =
          ForestChangeDiagnosticDataLossYearly.fill(
            dataGroup.umdTreeCoverLossYear,
            data.totalArea,
            dataGroup.isIdnForestMoratorium && dataGroup.isUMDLoss
          ),
        prodesLossYearly = ForestChangeDiagnosticDataLossYearly.fill(
          dataGroup.prodesLossYear,
          data.totalArea,
          dataGroup.isProdesLoss
        ),
        prodesLossProtectedAreasYearly =
          ForestChangeDiagnosticDataLossYearly.fill(
            dataGroup.prodesLossYear,
            data.totalArea,
            dataGroup.isProdesLoss && dataGroup.isProtectedArea
          ),
        prodesLossProdesPrimaryForestYearly =
          ForestChangeDiagnosticDataLossYearly.fill(
            dataGroup.prodesLossYear,
            data.totalArea,
            dataGroup.isProdesLoss && dataGroup.isPrimaryForest
          ),
        treeCoverLossBRABiomesYearly =
          ForestChangeDiagnosticDataLossYearlyCategory.fill(
            dataGroup.braBiomes,
            dataGroup.umdTreeCoverLossYear,
            data.totalArea,
            include = dataGroup.isUMDLoss
          ),
        treeCoverExtent = ForestChangeDiagnosticDataDouble
          .fill(data.totalArea, dataGroup.isTreeCoverExtent30),
        treeCoverExtentPrimaryForest = ForestChangeDiagnosticDataDouble.fill(
          data.totalArea,
          dataGroup.isTreeCoverExtent30 && dataGroup.isPrimaryForest
        ),
        treeCoverExtentProtectedAreas = ForestChangeDiagnosticDataDouble.fill(
          data.totalArea,
          dataGroup.isTreeCoverExtent30 && dataGroup.isProtectedArea
        ),
        treeCoverExtentPeatlands = ForestChangeDiagnosticDataDouble.fill(
          data.totalArea,
          dataGroup.isTreeCoverExtent30 && dataGroup.isPeatlands
        ),
        treeCoverExtentIntactForests = ForestChangeDiagnosticDataDouble.fill(
          data.totalArea,
          dataGroup.isTreeCoverExtent30 && dataGroup.isIntactForestLandscapes2000
        ),
        primaryForestArea = ForestChangeDiagnosticDataDouble
          .fill(data.totalArea, dataGroup.isPrimaryForest),
        intactForest2016Area = ForestChangeDiagnosticDataDouble
          .fill(data.totalArea, dataGroup.isIntactForestLandscapes2000),
        totalArea = ForestChangeDiagnosticDataDouble.fill(data.totalArea),
        protectedAreasArea = ForestChangeDiagnosticDataDouble
          .fill(data.totalArea, dataGroup.isProtectedArea),
        peatlandsArea = ForestChangeDiagnosticDataDouble
          .fill(data.totalArea, dataGroup.isPeatlands),
        braBiomesArea = ForestChangeDiagnosticDataDoubleCategory
          .fill(dataGroup.braBiomes, data.totalArea),
        idnForestAreaArea = ForestChangeDiagnosticDataDoubleCategory
          .fill(dataGroup.idnForestArea, data.totalArea),
        seAsiaLandCoverArea = ForestChangeDiagnosticDataDoubleCategory
          .fill(dataGroup.seAsiaLandCover, data.totalArea),
        idnLandCoverArea = ForestChangeDiagnosticDataDoubleCategory
          .fill(dataGroup.idnLandCover, data.totalArea),
        idnForestMoratoriumArea = ForestChangeDiagnosticDataDouble
          .fill(data.totalArea, dataGroup.isIdnForestMoratorium),
        southAmericaPresence = ForestChangeDiagnosticDataBoolean
          .fill(dataGroup.southAmericaPresence),
        legalAmazonPresence = ForestChangeDiagnosticDataBoolean
          .fill(dataGroup.legalAmazonPresence),
        braBiomesPresence = ForestChangeDiagnosticDataBoolean
          .fill(dataGroup.braBiomesPresence),
        cerradoBiomesPresence = ForestChangeDiagnosticDataBoolean
          .fill(dataGroup.cerradoBiomesPresence),
        seAsiaPresence =
          ForestChangeDiagnosticDataBoolean.fill(dataGroup.seAsiaPresence),
        idnPresence =
          ForestChangeDiagnosticDataBoolean.fill(dataGroup.idnPresence),
        filteredTreeCoverExtent = ForestChangeDiagnosticDataDouble
          .fill(
            data.totalArea,
            dataGroup.isTreeCoverExtent90 && !dataGroup.isPlantation
          ),
        filteredTreeCoverExtentYearly =
          ForestChangeDiagnosticDataValueYearly.empty,
        filteredTreeCoverLossYearly = ForestChangeDiagnosticDataLossYearly.fill(
          dataGroup.umdTreeCoverLossYear,
          data.totalArea,
          dataGroup.isUMDLoss && dataGroup.isTreeCoverExtent90 && !dataGroup.isPlantation
        ),
        filteredTreeCoverLossPeatYearly =
          ForestChangeDiagnosticDataLossYearly.fill(
            dataGroup.umdTreeCoverLossYear,
            data.totalArea,
            dataGroup.isUMDLoss && dataGroup.isTreeCoverExtent90 && !dataGroup.isPlantation && dataGroup.isPeatlands
          ),
        filteredTreeCoverLossProtectedAreasYearly =
          ForestChangeDiagnosticDataLossYearly.fill(
            dataGroup.umdTreeCoverLossYear,
            data.totalArea,
            dataGroup.isUMDLoss && dataGroup.isTreeCoverExtent90 && !dataGroup.isPlantation && dataGroup.isProtectedArea
          ),
        plantationArea = ForestChangeDiagnosticDataDouble
          .fill(data.totalArea, dataGroup.isPlantation),
        plantationOnPeatArea = ForestChangeDiagnosticDataDouble
          .fill(
            data.totalArea,
            dataGroup.isPlantation && dataGroup.isPeatlands
          ),
        plantationInProtectedAreasArea = ForestChangeDiagnosticDataDouble
          .fill(
            data.totalArea,
            dataGroup.isPlantation && dataGroup.isProtectedArea
          ),
        forestValueIndicator = ForestChangeDiagnosticDataValueYearly.empty,
        peatValueIndicator = ForestChangeDiagnosticDataValueYearly.empty,
        protectedAreaValueIndicator =
          ForestChangeDiagnosticDataValueYearly.empty,
        deforestationThreatIndicator =
          ForestChangeDiagnosticDataLossYearly.empty,
        peatThreatIndicator = ForestChangeDiagnosticDataLossYearly.empty,
        protectedAreaThreatIndicator =
          ForestChangeDiagnosticDataLossYearly.empty,
        fireThreatIndicator = ForestChangeDiagnosticDataLossYearly.empty
      )
    )

  }

  private def updateCommodityRisk(
                                   featureId: FeatureId,
                                   data: ForestChangeDiagnosticData
                                 ): (FeatureId, ForestChangeDiagnosticData) = {

    val minLossYear =
      ForestChangeDiagnosticDataLossYearly.prefilled.value.keysIterator.min

    val maxLossYear =
      ForestChangeDiagnosticDataLossYearly.prefilled.value.keysIterator.max

    val years: List[Int] = List.range(minLossYear + 1, maxLossYear + 1)

    val forestValueIndicator: ForestChangeDiagnosticDataValueYearly =
      ForestChangeDiagnosticDataValueYearly.fill(
        data.filteredTreeCoverExtent.value,
        data.filteredTreeCoverLossYearly.value,
        2
      )
    val peatValueIndicator: ForestChangeDiagnosticDataValueYearly =
      ForestChangeDiagnosticDataValueYearly.fill(data.peatlandsArea.value)
    val protectedAreaValueIndicator: ForestChangeDiagnosticDataValueYearly =
      ForestChangeDiagnosticDataValueYearly.fill(data.protectedAreasArea.value)
    val deforestationThreatIndicator: ForestChangeDiagnosticDataLossYearly =
      ForestChangeDiagnosticDataLossYearly(
        SortedMap(
          years.map(
            year =>
              (year, {

                // Somehow the compiler cannot infer the types correctly
                // I hence declare them here explicitly to help him out.
                val thisYearLoss: Double =
                data.filteredTreeCoverLossYearly.value
                  .getOrElse(year, 0)

                val lastYearLoss: Double =
                  data.filteredTreeCoverLossYearly.value
                    .getOrElse(year - 1, 0)

                thisYearLoss + lastYearLoss
              })
          ): _*
        )
      )
    val peatThreatIndicator: ForestChangeDiagnosticDataLossYearly =
      ForestChangeDiagnosticDataLossYearly(
        SortedMap(
          years.map(
            year =>
              (year, {
                // Somehow the compiler cannot infer the types correctly
                // I hence declare them here explicitly to help him out.
                val thisYearPeatLoss: Double =
                data.filteredTreeCoverLossPeatYearly.value
                  .getOrElse(year, 0)

                val lastYearPeatLoss: Double =
                  data.filteredTreeCoverLossPeatYearly.value
                    .getOrElse(year - 1, 0)

                thisYearPeatLoss + lastYearPeatLoss + data.plantationOnPeatArea.value

              })
          ): _*
        )
      )
    val protectedAreaThreatIndicator: ForestChangeDiagnosticDataLossYearly =
      ForestChangeDiagnosticDataLossYearly(
        SortedMap(
          years.map(
            year =>
              (year, {
                // Somehow the compiler cannot infer the types correctly
                // I hence declare them here explicitly to help him out.
                val thisYearProtectedAreaLoss: Double =
                data.filteredTreeCoverLossProtectedAreasYearly.value
                  .getOrElse(year, 0)

                val lastYearProtectedAreaLoss: Double =
                  data.filteredTreeCoverLossProtectedAreasYearly.value
                    .getOrElse(year - 1, 0)

                thisYearProtectedAreaLoss + lastYearProtectedAreaLoss + data.plantationInProtectedAreasArea.value
              })
          ): _*
        )
      )

    val new_data = data.update(
      forestValueIndicator = forestValueIndicator,
      peatValueIndicator = peatValueIndicator,
      protectedAreaValueIndicator = protectedAreaValueIndicator,
      deforestationThreatIndicator = deforestationThreatIndicator,
      peatThreatIndicator = peatThreatIndicator,
      protectedAreaThreatIndicator = protectedAreaThreatIndicator
    )
    (featureId, new_data)
  }

  private def getIntermediateRDD(
                                  intermediateListSource: NonEmptyList[String],
                                  spark: SparkSession,
                                  kwargs: Map[String, Any]
                                ): RDD[(FeatureId, ForestChangeDiagnosticData)] = {
    val intermediateDF = {
      FeatureDF(intermediateListSource, GfwProFeature, kwargs, spark)
    }

    intermediateDF.rdd.map(row => {
      val gfwproFeatureId = GfwProFeatureId(row.getString(0), row.getString(1).toInt)
      val gridId = GridId(row.getString(2))
      val treeCoverLossTcd30Yearly =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(3))
      val treeCoverLossPrimaryForestYearly =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(4))
      val treeCoverLossPeatLandYearly =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(5))
      val treeCoverLossIntactForestYearly =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(6))
      val treeCoverLossProtectedAreasYearly =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(7))
      val treeCoverLossSEAsiaLandCoverYearly =
        ForestChangeDiagnosticDataLossYearlyCategory.fromString(
          row.getString(8)
        )
      val treeCoverLossIDNLandCoverYearly =
        ForestChangeDiagnosticDataLossYearlyCategory.fromString(
          row.getString(9)
        )
      val treeCoverLossSoyPlanedAreasYearly =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(10))
      val treeCoverLossIDNForestAreaYearly =
        ForestChangeDiagnosticDataLossYearlyCategory.fromString(
          row.getString(11)
        )
      val treeCoverLossIDNForestMoratoriumYearly =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(12))
      val prodesLossYearly =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(13))
      val prodesLossProtectedAreasYearly =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(14))
      val prodesLossProdesPrimaryForestYearly =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(15))
      val treeCoverLossBRABiomesYearly =
        ForestChangeDiagnosticDataLossYearlyCategory.fromString(
          row.getString(16)
        )
      val treeCoverExtent =
        ForestChangeDiagnosticDataDouble(row.getString(17).toDouble)
      val treeCoverExtentPrimaryForest =
        ForestChangeDiagnosticDataDouble(row.getString(18).toDouble)
      val treeCoverExtentProtectedAreas =
        ForestChangeDiagnosticDataDouble(row.getString(19).toDouble)
      val treeCoverExtentPeatlands =
        ForestChangeDiagnosticDataDouble(row.getString(20).toDouble)
      val treeCoverExtentIntactForests =
        ForestChangeDiagnosticDataDouble(row.getString(21).toDouble)
      val primaryForestArea =
        ForestChangeDiagnosticDataDouble(row.getString(22).toDouble)
      val intactForest2016Area =
        ForestChangeDiagnosticDataDouble(row.getString(23).toDouble)
      val totalArea =
        ForestChangeDiagnosticDataDouble(row.getString(24).toDouble)
      val protectedAreasArea =
        ForestChangeDiagnosticDataDouble(row.getString(25).toDouble)
      val peatlandsArea =
        ForestChangeDiagnosticDataDouble(row.getString(26).toDouble)
      val braBiomesArea =
        ForestChangeDiagnosticDataDoubleCategory.fromString(row.getString(27))
      val idnForestAreaArea =
        ForestChangeDiagnosticDataDoubleCategory.fromString(row.getString(28))
      val seAsiaLandCoverArea =
        ForestChangeDiagnosticDataDoubleCategory.fromString(row.getString(29))
      val idnLandCoverArea =
        ForestChangeDiagnosticDataDoubleCategory.fromString(row.getString(30))
      val idnForestMoratoriumArea =
        ForestChangeDiagnosticDataDouble(row.getString(31).toDouble)
      val southAmericaPresence =
        ForestChangeDiagnosticDataBoolean(row.getString(32).toBoolean)
      val legalAmazonPresence =
        ForestChangeDiagnosticDataBoolean(row.getString(33).toBoolean)
      val braBiomesPresence =
        ForestChangeDiagnosticDataBoolean(row.getString(34).toBoolean)
      val cerradoBiomesPresence =
        ForestChangeDiagnosticDataBoolean(row.getString(35).toBoolean)
      val seAsiaPresence =
        ForestChangeDiagnosticDataBoolean(row.getString(36).toBoolean)
      val idnPresence =
        ForestChangeDiagnosticDataBoolean(row.getString(37).toBoolean)
      val forestValueIndicator =
        ForestChangeDiagnosticDataValueYearly.fromString(row.getString(38))
      val peatValueIndicator =
        ForestChangeDiagnosticDataValueYearly.fromString(row.getString(39))
      val protectedAreaValueIndicator =
        ForestChangeDiagnosticDataValueYearly.fromString(row.getString(40))
      val deforestationThreatIndicator =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(41))
      val peatThreatIndicator =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(42))
      val protectedAreaThreatIndicator =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(43))
      val fireThreatIndicator =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(44))

      val treeCoverLossTcd90Yearly =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(45))
      val filteredTreeCoverExtent =
        ForestChangeDiagnosticDataDouble(row.getString(46).toDouble)
      val filteredTreeCoverExtentYearly =
        ForestChangeDiagnosticDataValueYearly.fromString(row.getString(47))
      val filteredTreeCoverLossYearly =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(48))
      val filteredTreeCoverLossPeatYearly =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(49))
      val filteredTreeCoverLossProtectedAreasYearly =
        ForestChangeDiagnosticDataLossYearly.fromString(row.getString(50))
      val plantationArea =
        ForestChangeDiagnosticDataDouble(row.getString(51).toDouble)
      val plantationOnPeatArea =
        ForestChangeDiagnosticDataDouble(row.getString(52).toDouble)
      val plantationInProtectedAreasArea =
        ForestChangeDiagnosticDataDouble(row.getString(53).toDouble)

      (
        CombinedFeatureId(gfwproFeatureId, gridId),
        ForestChangeDiagnosticData(
          treeCoverLossTcd30Yearly,
          treeCoverLossTcd90Yearly,
          treeCoverLossPrimaryForestYearly,
          treeCoverLossPeatLandYearly,
          treeCoverLossIntactForestYearly,
          treeCoverLossProtectedAreasYearly,
          treeCoverLossSEAsiaLandCoverYearly,
          treeCoverLossIDNLandCoverYearly,
          treeCoverLossSoyPlanedAreasYearly,
          treeCoverLossIDNForestAreaYearly,
          treeCoverLossIDNForestMoratoriumYearly,
          prodesLossYearly,
          prodesLossProtectedAreasYearly,
          prodesLossProdesPrimaryForestYearly,
          treeCoverLossBRABiomesYearly,
          treeCoverExtent,
          treeCoverExtentPrimaryForest,
          treeCoverExtentProtectedAreas,
          treeCoverExtentPeatlands,
          treeCoverExtentIntactForests,
          primaryForestArea,
          intactForest2016Area,
          totalArea,
          protectedAreasArea,
          peatlandsArea,
          braBiomesArea,
          idnForestAreaArea,
          seAsiaLandCoverArea,
          idnLandCoverArea,
          idnForestMoratoriumArea,
          southAmericaPresence,
          legalAmazonPresence,
          braBiomesPresence,
          cerradoBiomesPresence,
          seAsiaPresence,
          idnPresence,
          filteredTreeCoverExtent,
          filteredTreeCoverExtentYearly,
          filteredTreeCoverLossYearly,
          filteredTreeCoverLossPeatYearly,
          filteredTreeCoverLossProtectedAreasYearly,
          plantationArea,
          plantationOnPeatArea,
          plantationInProtectedAreasArea,
          forestValueIndicator,
          peatValueIndicator,
          protectedAreaValueIndicator,
          deforestationThreatIndicator,
          peatThreatIndicator,
          protectedAreaThreatIndicator,
          fireThreatIndicator
        )
      )
    })
  }
}
