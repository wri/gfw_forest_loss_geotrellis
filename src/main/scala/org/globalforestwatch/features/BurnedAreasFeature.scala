package org.globalforestwatch.features

import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.sql.Row
import org.globalforestwatch.util.GeometryReducer

object BurnedAreasFeature extends Feature {
  override val geomPos: Int = 1

  val featureIdExpr = "alert__date as alertDate"
  val featureCount = 1

  def get(i: Row): geotrellis.vector.Feature[Geometry, FeatureId] = {
    val featureId = getFeatureId(i)
    val geom: Geometry =
      GeometryReducer.reduce(GeometryReducer.gpr)(
        WKB.read(i.getString(geomPos))
      )

    geotrellis.vector.Feature(geom, featureId)
  }

  def getFeatureId(i: Array[String], parsed: Boolean = false): FeatureId = {
    val alertDate = i(0)
    BurnedAreasFeatureId(alertDate)
  }
}
