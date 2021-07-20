package org.globalforestwatch.util

import com.vividsolutions.jts.geom.Envelope
import geotrellis.vector.Point
import org.apache.spark.sql.functions.{col, expr, lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import org.datasyslab.geosparksql.utils.Adapter
import org.globalforestwatch.grids.GridId.pointGridId

object GridDF {
  def apply(envelope: Envelope, spark: SparkSession): DataFrame = {

    val pointToGridId = udf((x: Double, y: Double) => pointGridId(Point(x, y), 1))

    //    spark.udf.register("pointToGridId", pointToGridId)

    val polygonRDD: PolygonRDD = GridRDD(envelope, spark)
    // TODO: fix gridID
    Adapter.toDf(polygonRDD, spark)
      //      .withColumn("x", expr("ST_X(ST_Centroid(geometry))"))
      //      .withColumn("y", expr("ST_Y(ST_Centroid(geometry))"))
      .withColumn("featureId", lit(0))
      //    pointToGridId(col("x"), col("y")) as "featureId"
      .select(col("geometry") as "polyshape", col("featureId"))
  }
}
