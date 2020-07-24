package org.globalforestwatch.util

import cats.Monoid
import geotrellis.raster.{CellGrid, Raster, RasterExtent}
import geotrellis.raster.histogram.StreamingHistogram
import geotrellis.util.GetComponent
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.globalforestwatch.features.FeatureId

import scala.reflect.runtime.universe.TypeTag

/** Here we define ad-hoc interface implementations.
  * These are interfaces required to perform polygonalSummary on a Raster[Tile]
  */
object Implicits {
  /** Instance of interface that defines how to:
    * - get an empty/initial value of StreamingHistogram
    * - combine two instances of StreamingHistogram
    *
    * This allows using polygonalSummary methods without providing initial value like:
    *
    * {{{
    * val summary = raster.polygonalSummary(geometry)
    * }}}
    *
    * @see https://typelevel.org/cats/typeclasses/monoid.html
    */
  implicit val streamingHistogramMonoid: Monoid[StreamingHistogram] =
    new Monoid[StreamingHistogram] {
      def empty: StreamingHistogram = new StreamingHistogram(256)
      def combine(x: StreamingHistogram, y: StreamingHistogram): StreamingHistogram = x.merge(y)
    }

  // Note: This implicit currently exists in geotrellis.contrib.polygonal.Implicits , which is already imported
  implicit def rasterHasRasterExtent[T <: CellGrid[Int]] = new GetComponent[Raster[T], RasterExtent] {
    override def get: Raster[T] => RasterExtent = { raster  => raster.rasterExtent }
  }

  implicit def newFeatureIdEncoder[T <: FeatureId : TypeTag]: Encoder[T] = ExpressionEncoder()

  implicit def bool2int(b: Boolean): Int = if (b) 1 else 0
}
