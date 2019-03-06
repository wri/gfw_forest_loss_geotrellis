package usbuildings

import java.net.URL

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.raster.FloatUserDefinedNoDataCellType
import geotrellis.vector.io._
import geotrellis.vector.io.wkt.WKT
import geotrellis.vector.{Feature, Polygon}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{HashPartitioner, SparkConf}

object StatsMain extends CommandApp (
  name = "geotrellis-usbuildings",
  header = "Collect building footprint elevations from terrain tiles",
  main = {
    val rasterOpt = Opts.options[String]("raster", "URI of raster")
    val sampleOpt = Opts.option[Double]("sample", help = "Fraction of input to sample").orNone

    val logger = Logger.getLogger(getClass)

    (rasterOpt, sampleOpt).mapN { (rasterUrl, sample) =>
      val conf = new SparkConf().
        setIfMissing("spark.master", "local[*]").
        setAppName("Building Footprint Elevation").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

      implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate

      val arr: Array[String] = rasterUrl.toList.toArray
      val rddOrUris: RDD[String] = spark.sparkContext.parallelize(arr, arr.length)

      val rddOfRasterSource = rddOrUris.map { grid =>
        RasterUtils.gridToRasterSources(grid)
      }
      val rddOfRasterTiles = rddOfRasterSource.flatMap { case (rs1, rs2, rs3) =>
        RasterUtils.rasterTiles(rs1, rs2, rs3)
      }
      val rddOfTuples = rddOfRasterTiles.flatMap{ case (r1, r2, r3) =>
        RasterUtils.rasterAsTable(r1, r2, r3)
      }

      import spark.sqlContext.implicits._
      val dataframe: DataFrame = rddOfTuples.toDF("col", "row", "loss_year", "tcd", "co2")


      import spark.sqlContext._
      import org.apache.spark.sql.functions._

      val ag_df =
        dataframe.groupBy("loss_year", "col").
          agg(count("*"), mean("tcd"), sum("co2"))

      ag_df.show()

//      outputDataFrame.write.
//        options(Map("header" -> "true", "delimiter" -> ",")).
//        csv(path = outputUrl)

      spark.stop
    }
  }
)


