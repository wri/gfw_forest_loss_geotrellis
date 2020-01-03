package org.globalforestwatch.features

import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.globalforestwatch.util.GeometryReducer
import org.globalforestwatch.util.Util._

object SimpleFeature extends Feature {

  val idPos = 0
  val geomPos = 1

  def get(
                  i: Row
                ): geotrellis.vector.Feature[Geometry, SimpleFeatureId] = {
    val feature_id: Int = i.getString(idPos).toInt
    val geom: Geometry =
      GeometryReducer.reduce(GeometryReducer.gpr)(
        WKB.read(i.getString(geomPos))
      )
    geotrellis.vector.Feature(geom, SimpleFeatureId(feature_id))
  }

  override def custom_filter(
                              filters: Map[String, Any]
                            )(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val idStart: Option[Int] = getAnyMapValue[Option[Int]](filters, "idStart")
    val idEnd: Option[Int] = getAnyMapValue[Option[Int]](filters, "idEnd")

    val idStartDF: DataFrame =
      idStart.foldLeft(df)((acc, i) => acc.filter($"fid" >= i))

    idEnd.foldLeft(idStartDF)((acc, i) => acc.filter($"fid" < i))

  }
}
