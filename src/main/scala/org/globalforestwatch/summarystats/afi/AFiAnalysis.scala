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
import breeze.linalg.svd

object AFiAnalysis extends SummaryAnalysis with SummaryDF {

  val name = "afi"

  def combOp(a: (Int, String, AFiData), b: (Int, String, AFiData)): (Int, String, AFiData) = {
    val status_code = if (a._1 >= b._1) a._1 else b._1
    val loc_error = if (a._2 != "" && a._2 != null) {
      if (b._2 != "" && b._2 != null) a._2 + ", " + b._2 else a._2
    } else  if (b._2 != "" && b._2 != null) b._2 else ""
    (status_code, loc_error, a._3.merge(b._3))
  }

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

    import spark.implicits._
    val summaryRDD: RDD[ValidatedLocation[AFiSummary]] = AFiRDD(validatedRDD, AFiGrid.blockTileGrid, kwargs)

    val baseRDD = AFiDF.getFeatureDataFrame(summaryRDD, spark)
    val reduceRDD = baseRDD.reduceByKey(combOp _)
    println("====")
    //reduceRDD.collect().foreach(println)
    val aggRDD = reduceRDD.filter(a => a._1.location_id == "-1").map(a => (AFiDF.RowGadmId(a._1.list_id, a._1.location_id, ""), a._2)).reduceByKey(combOp _)
    println("====")
    aggRDD.collect().foreach(println)
    println("====")
    val finalRDD = reduceRDD.union(aggRDD)
    finalRDD.collect().foreach(println)
    val combinedDF = finalRDD.toDF("key", "value").select(col("key.*"), col("value.*")).select(col("list_id"), col("location_id"), col("gadm_id"),  col("_3.*"), col("_1").as("status_code"), col("_2").as("location_error"))
    combinedDF.show(100, truncate=false)

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
