package org.globalforestwatch.stats.afi

import cats.Monoid
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
import org.locationtech.jts.geom._
//import org.locationtech.rasterframes._

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
    println(f"Number of splitted geoms: ${split.count}")

    // Key to z-index (optional?)
    // Partition by z-index (optional?)
    //   Skipping this for now, but the reason to do this is perhaps to improve the
    //   characteristics of distribution of spatial keys

    // Per spatial key, per partition(?):
    //   Grab the raster data for this window (errors possible)
    //   Poygonal summary on a per-key, per partition basis
    //   Add the per-key, per-id summaries to each row (as a Maybe, in case there are
    //     read errors)
    //
    // NOTE: I wanted to achieve this step as a custom Window aggregation function, but
    // I couldn't figure out how to do so
    val analyzer = AFiAnalyzer

    val summarized = split.repartition('key).as[AFiFeature].mapPartitions { iter =>
      val rasterCache = TrieMap.empty[SpatialKey, Maybe[List[Tile]]]
      val realized = analyzer.layers.map(RealizedLayer(_, grid))

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
            realized
              .map(_.read(key)) // Seq[RealizedLayer] -> Seq[Maybe[Tile]]
              .sequence // Seq[Maybe[Tile]] -> Maybe[Seq[Tile]] (error tiles supersede)
          )
          // NOTE: Some elements of tiles value may be null (missing optional sources)

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

    // Reduce the partial summaries per key
    // val summaryAgg = F.udaf(
    //   new MonoidAggregator[Maybe[Int], Maybe[AFiSummary], Maybe[AFiSummary]] {
    //     def convert(in: Maybe[Int]): Maybe[AFiSummary] = {Monoid[Maybe[AFiSummary]].empty}
    //     def finish(buf: Maybe[AFiSummary]): Maybe[AFiSummary] = buf
    //   }
    // )
    val summaryAgg = F.udaf(new BasicMonoidAggregator[Maybe[AFiSummary]])
    val combined = summarized
      .groupBy('id)
      .agg(summaryAgg('summary).as('summary))
      .unwrap("summary")

    combined.printSchema

    // Perform dataframe analysis/aggregation

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

}
