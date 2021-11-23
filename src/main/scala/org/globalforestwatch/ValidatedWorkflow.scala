package org.globalforestwatch

import cats._
import cats.data.Validated
import cats.data.Validated.{Valid, Invalid}
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Working with RDDs in context of Validated, where per-row operations can fail but errors must be preserved.
 * Once a row has errored it can not be processed further, but it must be carried through to final output.
 * This is done in carrying errors in a seprate RDD, which is unioned with additional errors in later steps.
 */
final case class ValidatedWorkflow[E, A](invalid: RDD[Validated.Invalid[E]], valid: RDD[A]){
  /** Transformation over Valid RDD that is not expected to fail */
  def mapValid[B: ClassTag](f: RDD[A] => RDD[B]): ValidatedWorkflow[E, B] = {
    ValidatedWorkflow(invalid, f(valid))
  }

  /** Transformation over Valid RDD that may fail per row, failures are reified into Validated */
  def mapValidToValidated[B: ClassTag](f: RDD[A] => RDD[Validated[E, B]]): ValidatedWorkflow[E, B] = {
    val verifiedOutput = f(valid)
    val ValidatedWorkflow(nextInvalid, nextValid) = ValidatedWorkflow(verifiedOutput)
    ValidatedWorkflow(invalid.union(nextInvalid), nextValid)
  }

  /** Unify the Valid and Invalid branches into a single RDD of Validated values */
  def unify: RDD[Validated[E, A]] = {
    val validSide = valid.map(Validated.valid[E, A])
    val invalidSide = invalid.asInstanceOf[RDD[Validated[E, A]]]
    validSide.union(invalidSide)
  }

  def flatMap[B](f: RDD[A] => ValidatedWorkflow[E, B]): ValidatedWorkflow[E, B] = {
    val nextInstance = f(valid)
    ValidatedWorkflow(invalid.union(nextInstance.invalid), nextInstance.valid)
  }
}

object ValidatedWorkflow {
  def apply[E, A: ClassTag](rdd: RDD[Validated[E, A]]): ValidatedWorkflow[E, A] = {
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    val valid = rdd.collect {
      case Valid(a) => a
    }
    val invalid = rdd.collect{
      case row: Invalid[E] => row
    }.repartition(math.max(1, math.log10(rdd.getNumPartitions).toInt))
    ValidatedWorkflow(invalid, valid)
  }
}