package org.globalforestwatch.aggregators

import cats.Monoid
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import scala.reflect.runtime.universe.TypeTag

abstract class MonoidAggregator[
  IN, BUF <: Product : Monoid : TypeTag, OUT <: Product : TypeTag
] extends Aggregator[Option[IN], Option[BUF], Option[OUT]] {
  def convert(input: IN): BUF
  def finish(buffer: Option[BUF]): Option[OUT]

  val zero = Some(implicitly[Monoid[BUF]].empty)

  def reduce(bufferOpt: Option[BUF], inputOpt: Option[IN]): Option[BUF] = {
    for {
      buffer <- bufferOpt
      input <- inputOpt
    } yield {
      implicitly[Monoid[BUF]].combine(convert(input), buffer)
    }
  }

  def merge(buffer1Opt: Option[BUF], buffer2Opt: Option[BUF]): Option[BUF] = {
    for {
      buffer1 <- buffer1Opt
      buffer2 <- buffer2Opt
    } yield {
      implicitly[Monoid[BUF]].combine(buffer1, buffer2)
    }
  }

  @transient private val bufferEncoderInternal = Encoders.product[Option[BUF]]
  @transient private val outputEncoderInternal = Encoders.product[Option[OUT]]

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Option[BUF]] = bufferEncoderInternal
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Option[OUT]] = outputEncoderInternal
}

class BasicMonoidAggregator[T <: Product : Monoid : TypeTag] extends MonoidAggregator[T, T, T] {
  def convert(input: T): T = input
  def finish(buffer: Option[T]): Option[T] = buffer
}
