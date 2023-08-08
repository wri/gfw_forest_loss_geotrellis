package org.globalforestwatch.summarystats.afi

import frameless.Injection
import io.circe.syntax._

case class AFiDataBoolean(value: Boolean)
  extends AFiDataParser[AFiDataBoolean] {
  def merge(
    other: AFiDataBoolean
  ): AFiDataBoolean = {
    AFiDataBoolean((value || other.value))
  }

  def toJson: String = {
    this.value.asJson.noSpaces
  }
}

object AFiDataBoolean {
  def empty: AFiDataBoolean =
    AFiDataBoolean(false)

  def fill(value: Boolean): AFiDataBoolean = {
    AFiDataBoolean(value)
  }

  implicit def injection: Injection[AFiDataBoolean, String] =
    Injection(_.toJson, s => AFiDataBoolean(s.toBoolean))
}

