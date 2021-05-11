package org.globalforestwatch.features

import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.sql.Row
import org.globalforestwatch.util.GeometryReducer

object BurnedAreasFeature extends Feature {
  override val geomPos: Int = 0

  val featureIdExpr = "burn_date as burnDate"
  val featureCount = 1

  def get(i: Row): geotrellis.vector.Feature[Geometry, FeatureId] = {
    val featureId = getFeatureId(i)
    val geom: Geometry =
      GeometryReducer.reduce(GeometryReducer.gpr)(
        WKB.read(i.getString(geomPos))
      )

    geotrellis.vector.Feature(geom, featureId)
  }

  def getFeatureId(i: Array[String]): FeatureId = {
    val alertDate = i(1)
    BurnedAreasFeatureId(alertDate)
  }
}
