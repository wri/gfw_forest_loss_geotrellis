package org.globalforestwatch.summarystats.afi

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated}
import geotrellis.vector.{Feature, Geometry}
import geotrellis.store.index.zcurve.Z2
import org.apache.spark.HashPartitioner
import org.globalforestwatch.features._
import org.globalforestwatch.summarystats._
import org.globalforestwatch.util.GeometryConstructor.createPoint
import org.globalforestwatch.util.{RDDAdapter, SpatialJoinRDD}
import org.globalforestwatch.util.RDDAdapter
import org.globalforestwatch.ValidatedWorkflow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


object AFiAnalysis extends SummaryAnalysis {

  val name = "aafi"

  def apply(
    featureRDD: RDD[ValidatedLocation[Geometry]],
    featureType: String,
    spark: SparkSession,
    kwargs: Map[String, Any]
  ): Unit = {
    featureRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // TODO invalid should map to job error somehow, probably using ValidatedWorkflow
    val validatedRDD = featureRDD.map{
      case Validated.Valid(Location(id, geom: Geometry)) => Feature(geom, id)
      case Validated.Invalid(Location(id, geom: Geometry)) => Feature(geom, id)
    }

    val summaryRDD = AFiRDD(validatedRDD, AFiGrid.blockTileGrid, kwargs)

    // TODO somehow convert AFiSummary to AFiData
    val summaryDF = AFiDF.getFeatureDataFrame(summaryRDD, spark)
    val runOutputUrl: String = getOutputUrl(kwargs)
    AFiExport.export(featureType, summaryDF, runOutputUrl, kwargs)
  }
}
