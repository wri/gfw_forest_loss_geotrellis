package org.globalforestwatch

import cats.kernel.Semigroup
import cats.data.Validated
import geotrellis.raster.summary.polygonal.PolygonalSummaryResult
import org.globalforestwatch.features._

package object summarystats {
  type Location[A] = Tuple2[FeatureId, A]
  type ValidatedRow[A] = Validated[JobError, A]
  type ValidatedSummary[A] = Validated[JobError, PolygonalSummaryResult[A]]

  type ValidatedLocation[A] = Validated[Location[JobError], Location[A]]
  type ValidatedLocationSummary[A] = Validated[Location[JobError], Location[PolygonalSummaryResult[A]]]

  implicit def summarySemigroup[SUMMARY <: Summary[SUMMARY]]: Semigroup[SUMMARY] = Semigroup.instance {
    case (s1, s2) => s1.merge(s2)
  }
}
