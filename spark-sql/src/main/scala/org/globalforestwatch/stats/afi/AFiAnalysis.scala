package org.globalforestwatch.stats.afi

import cats.data.NonEmptyList
import cats.syntax.functor._ // provides Maybe.map
import cats.syntax.traverse._ // provides sequence
import geotrellis.raster.{RasterExtent, Tile}
import geotrellis.vector._
import geotrellis.layer.SpatialKey
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}
import org.globalforestwatch.aggregators.BasicMonoidAggregator
import org.globalforestwatch.features.{FeatureFilter, GfwProFeatureId}
import org.globalforestwatch.layout.TenByTen30mGrid
import org.globalforestwatch.raster._
import org.globalforestwatch.udf._
import org.globalforestwatch.util.Maybe
import org.globalforestwatch.util.Maybe.MaybeMethods
import org.globalforestwatch.util.Util.getAnyMapValue
import org.locationtech.jts.geom._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.concurrent.TrieMap

object AFiAnalysis {
  def apply(
    featureUri: NonEmptyList[String],
    featureType: String,
    featureFilter: FeatureFilter,
    kwargs: Map[String, Any]
  )(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    // Read in raw data
    val sourceData = spark.read.options(
      Map(
        "sep"->"\t",
        "inferSchema"->"true",
        "header"->"true"
      )).csv(featureUri.head)
        .withColumn("id", createGfwProFeatureId('list_id, 'location_id))
        .drop("list_id", "location_id")

    sourceData.printSchema

    // Create an error entry when geometry fails to decode from WKB or when the
    // geometry is empty
    val geoms = sourceData
      .withColumn(
        "geom", maybeHexStringToGeom('geom) // String -> Maybe[Geometry]
      )
      .withColumn(
        "geom", filterEmptyMaybeGeom('geom) // Maybe[Geometry] -> Maybe[Geometry]
      )
      .unwrap("geom")

    geoms.printSchema
    println(f"Number of source geoms: ${geoms.count}")

    // Cut up geoms by layout
    val grid = TenByTen30mGrid
    val layout = grid.segmentTileGrid
    val split = geoms
      .withColumn("key", F.explode_outer(whenValid(keyGeomsByGrid(layout)('geom))))

    split.printSchema
    split.show
    println(f"Number of splitted geoms: ${split.count}")

    // Key to z-index (optional?)
    // Partition by z-index (optional?)
    //   Skipping this for now, but the reason to do this is perhaps to improve the
    //   characteristics of distribution of spatial keys on nodes

    // Per spatial key, per partition(?):
    //   Grab the raster data for this window (errors possible)
    //   Poygonal summary on a per-key, per partition basis
    //   Add the per-key, per-id summaries to each row (as a Maybe, in case there are
    //     read errors)
    //
    // NOTE: I wanted to achieve this step as a custom Window aggregation function, but
    // I couldn't figure out how to do so
    val summarized = split.repartition('key).as[AFiFeature].mapPartitions { iter =>
      val rasterCache = TrieMap.empty[SpatialKey, Maybe[List[Tile]]]
      val analyzer = AFiAnalyzer
      val realized = analyzer.layers.map(RealizedLayer(_, grid, "pro"))

      iter.map { feature =>
        if (feature.error.isEmpty) {
          assert(
            feature.key.isDefined, f"Feature with id ${feature.id} has no spatial key"
          )
          assert(
            feature.geom.isDefined, f"Feature with id ${feature.id} has no geometry"
          )

          val key = feature.key.get

          val rasterExtent = RasterExtent(
            layout.mapTransform.keyToExtent(key),
            layout.cellSize
          )
          val tiles: Maybe[List[Tile]] = rasterCache.getOrElseUpdate(
            key,
            realized            // Seq[RealizedLayer]
              .map(_.read(key)) // -> Seq[Maybe[Tile]]
              .sequence         // -> Maybe[Seq[Tile]] (error tiles supersede)
          )
          // NOTE: Some tiles in _.value may be null (missing optional sources)

          val summary: Maybe[AFiSummary] = tiles.map(
            analyzer.analyze(_, rasterExtent, feature.geom.get)
          )

          SegmentSummary(feature.id, feature.error, summary)
        } else {
          SegmentSummary(feature.id, feature.error, null)
        }
      }
    }.toDF

    summarized.printSchema
    summarized.show

    // Reduce the partial summaries per key
    val summaryAgg = F.udaf(new BasicMonoidAggregator[Maybe[AFiSummary]])

    val combined = summarized
      .groupBy('id)
      .agg(summaryAgg('summary).as('summary), F.collect_list('error).as('error))
      .withColumn("error", concat_array_outer(F.lit("\n"), 'error))
      .unwrap("summary")
      .withColumnRenamed("summary.stats", "summary")

    combined.printSchema

    // Perform dataframe analysis/aggregation
    val agged = combined
      .select('error, 'id, F.explode('summary))
      .withColumnRenamed("key", "gadm_id")
      .select('error, 'id, 'gadm_id, $"value.*")
      .persist

      // .withColumn(
      //   "gadm_id",
      //   maybeFilterByPattern(".*null.*", "Encountered invalid GADM tag")('gadm_id)
      // )
      // .unwrap("gadm_id")

    agged.printSchema

    // This is the important state.  We're already basically aggregated.  What is needed is to group the locationId==-1 records by listId, and merge all of these results irrespective of the gadm_id value.  So, maybe filter byIdByGadm by locationId=!=-1, put that aside, then isolate the locationId==-1 records and groupBy listId?  I'll have to tweak the gadm_id field to null (empty string?) in the latter before unioning the results.  Then we're done.

    val byLocation = agged
      .filter($"id.locationId" =!= -1)

    val byList = agged
      .groupBy($"id.listId")
      .agg(
        F.collect_list('error).as('error),
        F.sum("natural_forest__extent").alias("natural_forest__extent"),
        F.sum("natural_forest_loss__ha").alias("natural_forest_loss__ha"),
        F.sum("negligible_risk_area__ha").alias("negligible_risk_area__ha"),
        F.sum("total_area__ha").alias("total_area__ha")
      )
      .withColumn("gadm_id", F.typedLit[String](null))
      .withColumn("id", F.struct('listId, F.lit(-1).as('locationId)))
      .drop("listId")
      .withColumn("error", concat_array_outer(F.lit("\n"), 'error))

    byList.printSchema

    val result = byLocation.unionByName(byList)
      .withColumn(
        "negligible_risk__percent",
        $"negligible_risk_area__ha" / $"total_area__ha" * 100
      )
      .drop("negligible_risk_area__ha")

    val runOutputUrl: String = getOutputUrl(kwargs)

    spark.stop
  }

  case class AFiFeature(
    id: GfwProFeatureId,
    error: Option[String],
    geom: Option[Geometry],
    key: Option[SpatialKey]
  )

  case class SegmentSummary(id: GfwProFeatureId, error: Option[String], summary: Maybe[AFiSummary])

  val createGfwProFeatureId = F.udf { (list_id: Int, location_id: Int) =>
    GfwProFeatureId(list_id.toString, location_id)
  }

  def getOutputUrl(kwargs: Map[String, Any], outputName: String = "afi"): String = {
    val noOutputPathSuffix: Boolean = getAnyMapValue[Boolean](kwargs, "noOutputPathSuffix")
    val outputPath: String = getAnyMapValue[String](kwargs, "outputUrl")

    if (noOutputPathSuffix) outputPath
    else s"${outputPath}/${outputName}_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

  }
}
