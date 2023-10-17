package org.globalforestwatch.util

import geotrellis.vector.Extent

import scala.math.{pow, round}

class RoundedExtent(_xmin: Double,
                    _ymin: Double,
                    _xmax: Double,
                    _ymax: Double,
                    digits: Int)
    extends Extent(
      round(_xmin * pow(10, digits)) / pow(10, digits),
      round(_ymin * pow(10, digits)) / pow(10, digits),
      round(_xmax * pow(10, digits)) / pow(10, digits),
      round(_ymax * pow(10, digits)) / pow(10, digits)
    ) {

  val factor: Double = pow(10, digits)
  override val xmin: Double = round(_xmin * factor) / factor
  override val ymin: Double = round(_ymin * factor) / factor
  override val xmax: Double = round(_xmax * factor) / factor
  override val ymax: Double = round(_ymax * factor) / factor
  override val width: Double = round((xmax - xmin) * factor) / factor
  override val height: Double = round((ymax - ymin) * factor) / factor

}
