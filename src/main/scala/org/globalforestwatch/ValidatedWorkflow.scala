package org.globalforestwatch

import cats.data.Validated
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Working with RDDs in context of Validated, where per-row operations can fail but errors must be preserved.
 * Once a row has errored it can not be processed further, but it must be carried through to final output.
 * This is done in carrying errors in a separate RDD, which is unioned with additional errors in later steps.
 */
final case class ValidatedWorkflow[E, A](invalid: RDD[Validated.Invalid[E]], valid: RDD[A]){
  /** Transformation f over 'valid' RDD that is not expected to fail */
  def mapValid[B: ClassTag](f: RDD[A] => RDD[B]): ValidatedWorkflow[E, B] = {
    ValidatedWorkflow(invalid, f(valid))
  }

  /** Transformation f over 'valid' RDD that may fail per row, failures are reified
    * into the 'invalid' field of ValidatedWorkFlow */
  def mapValidToValidated[B: ClassTag](f: RDD[A] => RDD[Validated[E, B]]): ValidatedWorkflow[E, B] = {
    val verifiedOutput = f(valid)
    val ValidatedWorkflow(nextInvalid, nextValid) = ValidatedWorkflow(verifiedOutput)
    ValidatedWorkflow(invalid.union(nextInvalid), nextValid)
  }

  /** Unify the 'valid' and 'invalid' branches into a single RDD of Validated values */
  def unify: RDD[Validated[E, A]] = {
    val validSide = valid.map(Validated.valid[E, A])
    val invalidSide = invalid.asInstanceOf[RDD[Validated[E, A]]]
    validSide.union(invalidSide)
  }

  /** Transformation f over 'valid' RDD that may fail per row. Called flatMap because f
    * already yields a ValidatedWorkFlow. */
  def flatMap[B](f: RDD[A] => ValidatedWorkflow[E, B]): ValidatedWorkflow[E, B] = {
    val nextInstance = f(valid)
    ValidatedWorkflow(invalid.union(nextInstance.invalid), nextInstance.valid)
  }
}

object ValidatedWorkflow {
  def apply[E, A: ClassTag](rdd: RDD[Validated[E, A]]): ValidatedWorkflow[E, A] = {
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    val valid = rdd.collect {
      case Validated.Valid(a) => a
    }
    val invalid = rdd.collect{
      case row: Validated.Invalid[E] => row
    }.repartition(math.max(1, math.log10(rdd.getNumPartitions).toInt))
    ValidatedWorkflow(invalid, valid)
  }
}
