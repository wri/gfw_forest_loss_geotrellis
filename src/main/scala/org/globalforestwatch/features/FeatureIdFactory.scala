package org.globalforestwatch.features

case class FeatureIdFactory(featureType: String) {
  def featureId(id: Any): FeatureId =
    (featureType, id) match {
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

  def fromUserData(value: String): FeatureId = {

    val values = value.filterNot("[]".toSet).split(",").map(_.trim)

    featureType match {
      case "gadm" => ???

      case "feature" => SimpleFeatureId(values(0).toInt)
      case "wdpa" => ???

      case "geostore" => ???
      case _ =>
        throw new IllegalArgumentException(
          "Feature type must be one of 'gadm', 'wdpa', 'geostore' or 'feature'"
        )
    }
  }
}
