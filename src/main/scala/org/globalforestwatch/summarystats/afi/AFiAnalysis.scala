package org.globalforestwatch.summarystats.afi
import cats.data.Validated
import geotrellis.vector.{Feature, Geometry}
import org.globalforestwatch.features._
import org.globalforestwatch.summarystats._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object AFiAnalysis extends SummaryAnalysis {

  val name = "afi"

  def apply(
    featureRDD: RDD[ValidatedLocation[Geometry]],
    featureType: String,
    spark: SparkSession,
    kwargs: Map[String, Any]
  ): DataFrame = {
    featureRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // TODO invalid should map to job error somehow, probably using ValidatedWorkflow
    val validatedRDD = featureRDD.map {
      case Validated.Valid(Location(id, geom: Geometry))   => Feature(geom, id)
      case Validated.Invalid(Location(id, geom: Geometry)) => Feature(geom, id)
    }

    val summaryRDD: RDD[ValidatedLocation[AFiSummary]] = AFiRDD(validatedRDD, AFiGrid.blockTileGrid, kwargs)

    import spark.implicits._

    val combinedDF = AFiDF.getFeatureDataFrame(summaryRDD, spark)

    val resultsDF = combinedDF
       .withColumn(
         "negligible_risk__percent",
         $"negligible_risk_area__ha" / $"total_area__ha" * 100
       )
       .drop("negligible_risk_area__ha")

    resultsDF
      .withColumn("list_id", $"list_id".cast(IntegerType))
      .withColumn("location_id", $"location_id".cast(IntegerType))
  }
}
