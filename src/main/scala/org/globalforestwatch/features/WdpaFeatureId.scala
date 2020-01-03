package org.globalforestwatch.features

case class WdpaFeatureId(wdpaId: Int,
                         name: String,
                         iucnCat: String,
                         iso: String,
                         status: String)
    extends FeatureId {
  override def toString: String = s"$wdpaId"
}
