package org.globalforestwatch.features

import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.sql.Row
import org.globalforestwatch.util.GeometryReducer

object WdpaFeature extends Feature {

  val wdpaIdPos = 0
  val namePos = 1
  val iucnCatPos = 2
  val isoPos = 3
  val statusPos = 4
  val geomPos = 5

  def getFeature(i: Row): geotrellis.vector.Feature[Geometry, WdpaFeatureId] = {
    val wdpa_id: Int = i.getString(wdpaIdPos).toInt
    val name: String = i.getString(namePos)
    val iucn_cat: String = i.getString(iucnCatPos)
    val iso: String = i.getString(isoPos)
    val status: String = i.getString(statusPos)
    val geom: Geometry =
      GeometryReducer.reduce(GeometryReducer.gpr)(
        WKB.read(i.getString(geomPos))
      )
    geotrellis.vector
      .Feature(geom, WdpaFeatureId(wdpa_id, name, iucn_cat, iso, status))
  }

}
