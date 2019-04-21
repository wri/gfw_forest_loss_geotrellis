import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val conf = new SparkConf().
  setIfMissing("spark.master", "local[*]").
  setAppName("Tree Cover Loss DataFrame").
  set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
  set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator").
  set("spark.sql.crossJoin.enabled", "true")

implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate
import spark.implicits._

val WindowPartitionOrder = Window.partitionBy($"feature_id", $"layers").orderBy($"threshold".desc)
val myDF = Seq((1,1,0,0.3),
              (1,1,1,0.0),
              (1,1,2,0.3),
  (1, 1, 3, 0.1),
              (1,1,4,0.3),
  (2, 1, 5, 0.1),
              (2,1,0,0.3),
              (2,1,1,0.3),
              (2,1,2,0.3),
              (2,1,3,0.3),
              (2,1,4,0.3),
              (2,1,5,0.3)).toDF("feature_id", "layers", "threshold", "values")

def windowSum(col:String) = sum(col).over(WindowPartitionOrder)

myDF.orderBy("feature_id", "threshold").show(false)
myDF.select($"feature_id", $"layers", $"threshold", windowSum("values") as "values").orderBy("feature_id", "threshold").show(false)
myDF.groupBy($"feature_id", $"layers").agg(sum($"threshold"), sum($"threshold" * $"values"), sum($"threshold" * $"values") / sum($"threshold"), avg($"values")).show(false)



