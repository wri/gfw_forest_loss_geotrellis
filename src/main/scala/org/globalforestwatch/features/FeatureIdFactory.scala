package org.globalforestwatch.features

case class FeatureIdFactory(featureName: String) {
  def featureId(id: Any): FeatureId =
    (featureName, id) match {
      case ("gadm", (iso: String, adm1: Integer, adm2: Integer)) =>
        GadmFeatureId(iso, adm1, adm2)
      case ("feature", simple_id: Int) => SimpleFeatureId(simple_id)
      case (
        "wdpa",
        (
          wdpaId: Int,
          name: String,
          iucnCat: String,
          iso: String,
          status: String
          )
        ) =>
        WdpaFeatureId(wdpaId, name, iucnCat, iso, status)
      case ("geostore", geostore: String) => GeostoreFeatureId(geostore)
      case _ =>
        throw new IllegalArgumentException(
          "Feature type must be one of 'gadm', 'wdpa', 'geostore' or 'feature'"
        )
    }
}
