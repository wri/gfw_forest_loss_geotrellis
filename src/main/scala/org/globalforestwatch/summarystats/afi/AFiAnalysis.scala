package org.globalforestwatch.summarystats.afi
import org.apache.spark.sql.functions.{col, lit, when, sum, max, concat_ws, collect_list}
import cats.data.Validated
import geotrellis.vector.{Feature, Geometry}
import org.globalforestwatch.features._
import org.globalforestwatch.summarystats._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
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

    val baseDF = AFiDF
        .getFeatureDataFrame(summaryRDD, spark)
    println("UUUU BaseDF", baseDF.schema)
    baseDF.show(50, truncate = false)
    // Null out gadm_id for all non-dissolved rows and then aggregate all results for
    // each unique (list_id, location_id, gadm_id).
    val summaryDF = AFiAnalysis.aggregateResults(
        baseDF
        .withColumn(
          "gadm_id", when(col("location_id") =!= -1, lit("") ).otherwise(col("gadm_id"))
        )
        .groupBy(col("list_id"), col("location_id"), col("gadm_id"))
    )
    println("UUUU SummaryDF")
    summaryDF.show(50, truncate = false)

    // For each unique list_id, aggregate all dissolved rows with that list_id, and
    // create a summary row (list_id, -1, "").
    val gadmAgg = AFiAnalysis.aggregateResults(
      summaryDF
      .filter(col("location_id") === -1)
      .groupBy(col("list_id")),
    )
      .withColumn("gadm_id", lit(""))
      .withColumn("location_id", lit(-1))

    // Add in the summary rows.
    val combinedDF = summaryDF.unionByName(gadmAgg)
    // Replace neglibible_risk_area__ha by neglible_risk__percent.
    val resultsDF = combinedDF
      .withColumn(
        "negligible_risk__percent",
        col("negligible_risk_area__ha") / col("total_area__ha") * 100
      )
      .drop("negligible_risk_area__ha")

    resultsDF
      .withColumn("list_id", col("list_id").cast(IntegerType))
      .withColumn("location_id", col("location_id").cast(IntegerType))
  }

  private def aggregateResults(group: RelationalGroupedDataset) = {
    group.agg(
        sum("natural_forest__extent").alias("natural_forest__extent"),
        sum("natural_forest_loss__ha").alias("natural_forest_loss__ha"),
        sum("natural_forest_loss_by_year__ha").alias("natural_forest_loss_by_year__ha"),
        sum("jrc_forest_cover__extent").alias("jrc_forest_cover__extent"),
        sum("jrc_forest_cover_loss__ha").alias("jrc_forest_cover_loss__ha"),
        sum("jrc_forest_cover_loss_by_year__ha").alias("jrc_forest_cover_loss_by_year__ha"),
        sum("negligible_risk_area__ha").alias("negligible_risk_area__ha"),
        sum("total_area__ha").alias("total_area__ha"),
        max("status_code").alias("status_code"),
        concat_ws(", ", collect_list(when(col("location_error").isNotNull && col("location_error") =!= "", col("location_error")))).alias("location_error")
      )
  }
}
