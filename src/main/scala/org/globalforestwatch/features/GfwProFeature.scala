package org.globalforestwatch.features

object GfwProFeature extends Feature {
  val listIdPos = 0
  val locationIdPos = 1
  val geomPos = 2

  val featureIdExpr = "list_id as listId, cast(location_id as int) as locationId"

  def getFeatureId(i: Array[String], parsed: Boolean = false): FeatureId = {

    val listId: String = i(0)
    val locationId: Int = i(1).toInt

    GfwProFeatureId(listId, locationId)
  }
}
