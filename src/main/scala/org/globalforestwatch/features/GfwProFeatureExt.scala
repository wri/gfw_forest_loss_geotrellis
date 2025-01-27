package org.globalforestwatch.features

// GfwPro Feature that includes commodity and yield columns for GHG analysis.

object GfwProFeatureExt extends Feature {
  val listIdPos = 0
  val locationIdPos = 1
  val geomPos = 2
  val commodityPos = 3
  val yieldPos = 4

  val featureIdExpr = "list_id as listId, cast(location_id as int) as locationId, commodity as commodity, yield as yield"

  def getFeatureId(i: Array[String], parsed: Boolean = false): FeatureId = {

    val listId: String = i(0)
    val locationId: Int = i(1).toInt
    val commodity: String = i(2)
    val yieldVal: Float = i(3).toFloat

    GfwProFeatureExtId(listId, locationId, commodity, yieldVal)
  }
}
