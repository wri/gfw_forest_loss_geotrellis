package usbuildings

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import com.monovore.decline.{CommandApp, Opts}
import cats.implicits._

import org.apache.log4j.Logger
import geotrellis.contrib.vlm.RasterSource
import geotrellis.raster.{MultibandTile, Raster, Tile}
import geotrellis.raster.histogram.{StreamingHistogram}
import geotrellis.spark.SpatialKey
import geotrellis.vector.{Feature, Polygon}
import geotrellis.vector.io._
import geotrellis.vector.io.wkt.WKT
import org.apache.spark.rdd.RDD

object Main extends CommandApp (
  name = "geotrellis-usbuildings",
  header = "Collect building footprint elevations from terrain tiles",
  main = {
    val featuresOpt = Opts.option[String]("features", help = "URI of the features CSV files")
    val outputOpt = Opts.option[String]("output", help = "URI of the output features CSV files")
    val limitOpt = Opts.option[Int]("limit", help = "Limit number of records processed")

    val logger = Logger.getLogger(getClass)

    (featuresOpt, outputOpt).mapN { (featuresUrl, outputUrl) =>
      val conf = new SparkConf().
        setIfMissing("spark.master", "local[*]").
        setAppName("Building Footprint Elevation").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

      implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate

      /* Use spark DataFrame API to read input CSV file
       * https://github.com/databricks/spark-csv
       */
      var features: DataFrame = spark.read.
        options(Map("header" -> "true", "delimiter" -> ",")).
        csv(path = featuresUrl)

      // If limit is defined apply it on input.
      limitOpt.map { n: Int => features = features.limit(n) }

      /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
      val featureRDD: RDD[Feature[Polygon, FeatureId]] =
        features.rdd.map { row: Row =>
          val state: String = row.getString(0)
          val id: Int = row.getString(1).toInt
          val wkt: String = row.getString(2)

          val geom: Polygon = WKT.read(wkt).asInstanceOf[Polygon]
          Feature(geom, FeatureId(state, id))
        }

      /* Increasing number of partitions produces better work distribution and increases reliability */
      val partitioner = new HashPartitioner(partitions = featureRDD.getNumPartitions * 16)

      /* Intersect features with NED tile grid and generate a record for each intersection.
       * When a feature is on tile edges it may intersect multiple tiles.
       * Later we will calculate partial result for each intersection and merge them.
       */
      val keyedFeatureRDD: RDD[(SpatialKey, Feature[Polygon, FeatureId])] =
        featureRDD.flatMap { feature =>
          // set of tile grid keys that intersect this particular feature
          val keys: Set[SpatialKey] = NED.tileGrid.mapTransform.keysForGeometry(feature.geom)
          keys.map { key => (key, feature) }
        }.partitionBy(partitioner)

      /* Here we're going to work with the features one partition at a time.
       * We're going to use the tile grid key to read pixels from appropriate raster.
       * Each record in this RDD may still represent only a partial result for that feature.
       *
       * The RDD is keyed by FeatureId such that we can join and recombine partial results later.
       */
      val featuresWithSummaries: RDD[(FeatureId, Feature[Polygon, FeatureProperties])] =

        keyedFeatureRDD.mapPartitions { featurePartition =>
          // Code inside .mapPartitions works in an Iterator of records
          // Doing things this way allows us to reuse resources and perform other optimizations

          // Grouping by spatial key allows us to minimize reading thrashing from record to record
          val groupedByKey: Map[SpatialKey, Array[(SpatialKey,Feature[Polygon, FeatureId])]] =
            featurePartition.toArray.groupBy(_._1)

          groupedByKey.toIterator.flatMap { case (tileKey, features) =>
            val rasterSource: RasterSource = NED.getRasterSource(tileKey)
            logger.info(s"Loading: ${rasterSource.uri} for $tileKey")

            features.map { case (_, feature) =>
              // Result is optional because we may have read a non-intersecting extent
              val maybeRaster: Option[Raster[MultibandTile]] = rasterSource.read(feature.envelope)

              require(rasterSource.extent.intersects(feature.envelope), {
                val tileWKT = tileKey.extent(NED.tileGrid).toPolygon().toWKT
                val wktSource = rasterSource.extent.toPolygon().toWKT
                s"$tileKey: $tileWKT does not intersect RasterSource(${rasterSource.uri}): $wktSource"
              })

              val id: FeatureId = feature.data

              maybeRaster match {
                case Some(raster) =>
                  val firstBandRaster: Raster[Tile] = raster.mapTile(_.band(0))

                  // these imports are needed for .polygonalSummary call (to be hidden up)
                  import geotrellis.contrib.polygonal.PolygonalSummary.ops._
                  import geotrellis.contrib.polygonal.Implicits._
                  import usbuildings.Implicits._

                  val Feature(geom, histogram) =
                    firstBandRaster.polygonalSummary[Polygon, StreamingHistogram](feature.geom)

                  (id, Feature(geom, FeatureProperties(id, Some(histogram))))

                case None =>
                  (id, Feature(feature.geom, FeatureProperties(id, None)))
              }
            }
          }
        }

      /* Group records by FeatureId and combine their summaries.
       * This RDD
       */
      val featuresGroupedWithSummaries: RDD[(FeatureId, Feature[Polygon, FeatureProperties])] =
        featuresWithSummaries.reduceByKey { case (Feature(geom, payload1), Feature(_, payload2)) =>
          Feature(geom, payload1.merge(payload2))
        }

      import spark.implicits._
      val outputDataFrame: DataFrame =
        featuresGroupedWithSummaries.map { case (_, feature) =>
          val id = feature.data.featureId
          val (min: Double, max: Double) = feature.data.histogram.
            flatMap(_.minMaxValues()).
            getOrElse((Double.NaN, Double.NaN))

          (id.state, id.index, min, max, feature.geom.toWKT)
        }.toDF("state", "index", "min", "max", "polygon")


      outputDataFrame.write.
        options(Map("header" -> "true", "delimiter" -> ",")).
        csv(path = outputUrl)

      // TODO: Add tests that explain what is going on

      spark.stop
    }
  }
)
