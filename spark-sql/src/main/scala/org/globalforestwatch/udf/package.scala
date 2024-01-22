package org.globalforestwatch

import geotrellis.vector._
import geotrellis.layer.LayoutDefinition
import org.apache.spark.sql.{Column, functions => F}
import org.globalforestwatch.features.SpatialFeatureDF
import org.globalforestwatch.util.{GeotrellisGeometryValidator, Maybe}
import org.locationtech.jts.geom.util.GeometryFixer

package object udf {
  def hexStringToGeomOption = F.udf{ hex: String =>
    SpatialFeatureDF.readOption(hex) match {
      case Some(g) =>
        Some(GeotrellisGeometryValidator.preserveGeometryType(
          GeometryFixer.fix(g), g.getGeometryType
        ))
      case None => None
    }
  }

  def maybeHexStringToGeom = F.udf{ hex: String =>
    Maybe.option("Failed to convert WKB string input") {
      SpatialFeatureDF.readOption(hex).map(g =>
        GeotrellisGeometryValidator.preserveGeometryType(
          GeometryFixer.fix(g), g.getGeometryType
        )
      )
    }
  }

  def filterEmptyMaybeGeom = F.udf{ geom: Maybe[Geometry] =>
    geom.assert({g: Geometry => !g.isEmpty}, "Geometry was empty")
  }

  def keyGeomsByGrid(layout: LayoutDefinition) = F.udf{ (geom: Geometry) =>
    layout.mapTransform.keysForGeometry(geom).toArray
  }

  def whenValid(expr: Column, errorColumnName: String = "error"): Column =
    F.when(F.isnull(F.col(errorColumnName)), expr).otherwise(null)

  def concat_outer = F.udf{ (sep: String, s1: String, s2: String) => (s1, s2) match {
    case (null, null) => null
    case (s, null) => s
    case (null, s) => s
    case (a, b) => f"${a}${sep}{$b}"
  }}

  def concat_array_outer = F.udf{ (sep: String, array: Array[String]) =>
    if (array.isEmpty)
      null: String
    else
      array.mkString(sep)
  }

  def maybeFilterByPattern(pattern: String, errorMessage: String) = F.udf{ str: String =>
    if (pattern.r.findFirstMatchIn(str).isDefined)
      Maybe(str)
    else
      Maybe.error[String](errorMessage)
  }
}
