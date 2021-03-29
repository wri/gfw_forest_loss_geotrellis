package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.data.NonEmptyList

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import geotrellis.vector.{Feature, Geometry}
import com.vividsolutions.jts.geom.{Geometry => GeoSparkGeometry}
import org.apache.log4j.Logger
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.features.{
  FeatureIdFactory,
  FireAlertRDD,
  SpatialFeatureDF
}

import java.util

//import org.apache.sedona.core.enums.{FileDataSplitter, GridType, IndexType}
//import org.apache.sedona.core.spatialOperator.JoinQuery
//import org.apache.sedona.core.spatialRDD.PointRDD
//import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.{
  FeatureDF,
  FeatureFactory,
  FeatureId,
  SimpleFeatureId
}
import org.globalforestwatch.util.Util.getAnyMapValue

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

object ForestChangeDiagnosticAnalysis {

  val logger = Logger.getLogger("ForestChangeDiagnosticAnalysis")

  def apply(featureRDD: RDD[Feature[Geometry, FeatureId]],
            featureType: String,
            spark: SparkSession,
            kwargs: Map[String, Any]): Unit = {

    val summaryRDD: RDD[(FeatureId, ForestChangeDiagnosticSummary)] =
      ForestChangeDiagnosticRDD(
        featureRDD,
        ForestChangeDiagnosticGrid.blockTileGrid,
        kwargs
      )

    val fireCount: RDD[(FeatureId, ForestChangeDiagnosticDataLossYearly)] =
      ForestChangeDiagnosticAnalysis.fireStats(featureType, spark, kwargs)

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

    val summaryDF =
      ForestChangeDiagnosticDFFactory(featureType, dataRDD, spark, kwargs).getDataFrame

    val runOutputUrl: String = getAnyMapValue[String](kwargs, "outputUrl") +
      "/forest_change_diagnostic_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

    ForestChangeDiagnosticExport.export(
      featureType,
      summaryDF,
      runOutputUrl,
      kwargs
    )
  }

  def fireStats(
                 featureType: String,
                 spark: SparkSession,
                 kwargs: Map[String, Any]
               ): RDD[(FeatureId, ForestChangeDiagnosticDataLossYearly)] = {

    // FIRE RDD
    val fireAlertSpatialRDD = FireAlertRDD(spark, kwargs)

    // Feature RDD
    val featureObj = FeatureFactory(featureType).featureObj
    val featureUris: NonEmptyList[String] =
      getAnyMapValue[NonEmptyList[String]](kwargs, "featureUris")
    val featurePolygonDF =
      SpatialFeatureDF(
        featureUris,
        featureObj,
        featureType,
        kwargs,
        spark,
        "geom"
      )
    val featureSpatialRDD = Adapter.toSpatialRdd(featurePolygonDF, "polyshape")

    featureSpatialRDD.analyze()

    // Join Fire and Feature RDD
    val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
    val considerBoundaryIntersection = false // Only return gemeotries fully covered by each query window in queryWindowRDD
    val usingIndex = false

    fireAlertSpatialRDD.spatialPartitioning(GridType.QUADTREE)

    featureSpatialRDD.spatialPartitioning(fireAlertSpatialRDD.getPartitioner)
    featureSpatialRDD.buildIndex(
      IndexType.QUADTREE,
      buildOnSpatialPartitionedRDD
    )

    val pairRDD = JoinQuery.SpatialJoinQuery(
      fireAlertSpatialRDD,
      featureSpatialRDD,
      usingIndex,
      considerBoundaryIntersection
    )

    pairRDD.rdd
      .map {
        case (poly, points) =>
          toForestChangeDiagnosticFireData(featureType, poly, points)
      }
      .reduceByKey(_ merge _)
      .mapValues { fires =>
        aggregateFireData(fires)
      }
  }

  private def toForestChangeDiagnosticFireData(
                                                featureType: String,
                                                poly: GeoSparkGeometry,
                                                points: util.HashSet[GeoSparkGeometry]
                                              ): (FeatureId, ForestChangeDiagnosticDataLossYearly) = {
    ( {
      val id = {
        poly.getUserData.asInstanceOf[String].filterNot("[]".toSet).toInt

      }
      FeatureIdFactory(featureType).featureId(id)

    }, {
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
              id match {
                case featureId: SimpleFeatureId =>
                  toForestChangeDiagnosticData(featureId, dataGroup, data)
                case _ =>
                  throw new IllegalArgumentException("Not a SimpleFeatureId")
              }
          }
      }

  }

  private def aggregateFireData(
                                 fires: ForestChangeDiagnosticDataLossYearly
                               ): ForestChangeDiagnosticDataLossYearly = {
    val minLossYear = fires.value.keysIterator.min
    val maxLossYear = fires.value.keysIterator.max
    val years: List[Int] = List.range(minLossYear + 1, maxLossYear + 1)

    ForestChangeDiagnosticDataLossYearly(
      SortedMap(
        years.map(
          year =>
            (year, { // I hence declare them here explicitly to help him out.
              val thisYearLoss: Double = fires.value.getOrElse(year, 0)
              val lastYearLoss: Double = fires.value.getOrElse(year - 1, 0)
              (thisYearLoss + lastYearLoss) / 2
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
            dataGroup.isIntactForestLandscapes2016 && dataGroup.isUMDLoss
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
          dataGroup.isTreeCoverExtent30 && dataGroup.isIntactForestLandscapes2016
        ),
        primaryForestArea = ForestChangeDiagnosticDataDouble
          .fill(data.totalArea, dataGroup.isPrimaryForest),
        intactForest2016Area = ForestChangeDiagnosticDataDouble
          .fill(data.totalArea, dataGroup.isIntactForestLandscapes2016),
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
        filteredTreeCoverExtentYearly =
          ForestChangeDiagnosticDataValueYearly.fill(
            dataGroup.umdTreeCoverLossYear,
            data.totalArea,
            dataGroup.isTreeCoverExtent90 && !dataGroup.isPlantation
          ),
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
      data.treeCoverLossTcd90Yearly.value.keysIterator.min
    val maxLossYear =
      data.treeCoverLossTcd90Yearly.value.keysIterator.max
    val years: List[Int] = List.range(minLossYear + 1, maxLossYear + 1)

    val forestValueIndicator: ForestChangeDiagnosticDataValueYearly =
      data.filteredTreeCoverExtentYearly
    val peatValueIndicator: ForestChangeDiagnosticDataValueYearly =
      ForestChangeDiagnosticDataValueYearly.fill(0, data.peatlandsArea.value)
    val protectedAreaValueIndicator: ForestChangeDiagnosticDataValueYearly =
      ForestChangeDiagnosticDataValueYearly.fill(
        0,
        data.protectedAreasArea.value
      )
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
}
