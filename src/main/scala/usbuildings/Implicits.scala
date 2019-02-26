package usbuildings

import cats._
import geotrellis.contrib.polygonal.CellAccumulator
import geotrellis.raster.histogram.{Histogram, StreamingHistogram}

object Implicits {
  implicit val ev0: Monoid[StreamingHistogram] = new Monoid[StreamingHistogram] {
    override def empty: StreamingHistogram = new StreamingHistogram(256)
    override def combine(x: StreamingHistogram, y: StreamingHistogram): StreamingHistogram = x.merge(y)
  }
  implicit val ev1: CellAccumulator[StreamingHistogram] = new CellAccumulator[StreamingHistogram] {
    override def add(self: StreamingHistogram, v: Int): StreamingHistogram = {
      self.countItem(v, 1)
      self
    }

    override def add(self: StreamingHistogram, v: Double): StreamingHistogram = {
      self.countItem(v, 1)
      self
    }
  }
}
