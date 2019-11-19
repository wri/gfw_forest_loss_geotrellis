package org.globalforestwatch.features

import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.globalforestwatch.util.GeometryReducer
import org.globalforestwatch.util.Util._

object GeostoreFeature extends Feature {

  val idPos = 0
  val geomPos = 1

  def getFeature(
    i: Row
  ): geotrellis.vector.Feature[Geometry, GeostoreFeatureId] = {
    val geostore_id: String = i.getString(idPos)
    val geom: Geometry =
      GeometryReducer.reduce(GeometryReducer.gpr)(
        WKB.read(i.getString(geomPos))
      )
    geotrellis.vector.Feature(geom, GeostoreFeatureId(geostore_id))
  }

  def filter(filters: Map[String, Any])(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    var newDF = df

    //  TODO possible future option
    //  val geostoreId: Option[String] =
    //    getAnyMapValue[Option[String]](filters, "geostoreId")
    val limit: Option[Int] = getAnyMapValue[Option[Int]](filters, "limit")

    //  geostoreId.foreach { id =>
    //    newDF = newDF.filter($"geostore_id" === id)
    //  }

    limit.foreach { n =>
      newDF = newDF.limit(n)
    }

    newDF
  }
}
