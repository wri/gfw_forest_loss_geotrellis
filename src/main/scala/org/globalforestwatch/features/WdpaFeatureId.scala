package org.globalforestwatch.features

case class WdpaFeatureId(wdpa_id: Int,
                         name: String,
                         iucn_cat: String,
                         iso: String,
                         status: String)
    extends FeatureId {
  override def toString: String = s"$wdpa_id"
}
