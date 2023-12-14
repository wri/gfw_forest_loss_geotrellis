package org.globalforestwatch.aggregators

import cats.Monoid
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator

abstract class MonoidAggregator[
  IN, BUF : Monoid: Encoder, OUT: Encoder
] extends Aggregator[IN, BUF, OUT] {
  def convert(input: IN): BUF
  def finish(buffer: BUF): OUT

  val zero = implicitly[Monoid[BUF]].empty

  def reduce(buffer: BUF, input: IN): BUF = implicitly[Monoid[BUF]].combine(
    convert(input), buffer
  )

  def merge(buffer1: BUF, buffer2: BUF): BUF = implicitly[Monoid[BUF]].combine(
    buffer1, buffer2
  )

  @transient private val bufferEncoderInternal = implicitly[Encoder[BUF]]
  @transient private val outputEncoderInternal = implicitly[Encoder[OUT]]

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[BUF] = bufferEncoderInternal
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[OUT] = outputEncoderInternal
}

class BasicMonoidAggregator[T: Monoid: Encoder] extends MonoidAggregator[T, T, T] {
  def convert(input: T): T = input
  def finish(buffer: T): T = buffer
}
