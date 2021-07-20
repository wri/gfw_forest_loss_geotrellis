package org.globalforestwatch.features

case class GadmFeatureId(iso: String, adm1: Integer, adm2: Integer) extends FeatureId {
  override def toString: String = s"$iso.$adm1.$adm2"
}
