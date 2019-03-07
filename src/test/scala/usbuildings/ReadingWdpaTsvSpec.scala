//package usbuildings
//
//import geotrellis.spark.testkit.TestEnvironment
//import geotrellis.vector.io.wkt.WKT
//import geotrellis.vector.{Feature, Geometry, Polygon}
//import org.scalatest._
//import org.apache.spark._
//import org.apache.spark.rdd._
//import org.apache.spark.sql._
//import org.apache.spark.sql.functions._
//
//
//class ReadingWdpaTsvSpec extends FunSpec with TestEnvironment {
//  override lazy val _ssc: SparkSession = {
//    val conf = new SparkConf()
//    conf
//      .setMaster("local[16]")
//      .setAppName("Test Context")
//      .set("spark.default.parallelism", "8")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
//      .set("spark.kryoserializer.buffer.max", "500m")
//
//    SparkSession.builder().config(conf).getOrCreate()
//  }
//
//  val spark: SparkSession = this._ssc
//
//  val uri: String = "file:/Users/eugene/wdpa__10N_010E.tsv"
//
//  var features: DataFrame = spark.read.
//    options(Map("header" -> "false", "delimiter" -> "\t")).
//    csv(path = uri).limit(1)
//
//
//
//  /* Transition from DataFrame to RDD in order to work with GeoTrellis features */
//  val featureRDD: RDD[Feature[Geometry, Id]] =
//    features.rdd.map { row: Row =>
//      val areaType = row.getString(1)
//      val countryCode: String = row.getString(6)
//      val admin1: Int = row.getString(7).toInt
//      val admin2: Int = row.getString(8).toInt
//      val geom: Geometry = WKT.read(row.getString(0))
//      Feature(geom, Id(areaType, countryCode, admin1, admin2))
//    }//.sample(false, 0.001)
//
//
//  val partitioner = new HashPartitioner(partitions = featureRDD.getNumPartitions * 128)
//  val result = TreeLossJob.apply(featureRDD, partitioner)
//
//  result.take(100).foreach(println)
//}
