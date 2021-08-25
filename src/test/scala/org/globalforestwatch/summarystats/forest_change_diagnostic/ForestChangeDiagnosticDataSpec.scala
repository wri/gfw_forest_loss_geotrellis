package org.globalforestwatch.summarystats.forest_change_diagnostic

import org.globalforestwatch.TestEnvironment
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StringType


case class Boop(
  a: ForestChangeDiagnosticDataDouble,
  b: ForestChangeDiagnosticDataBoolean,
  c: ForestChangeDiagnosticDataLossYearly,
  d: ForestChangeDiagnosticDataValueYearly,
  e: ForestChangeDiagnosticDataDoubleCategory)

class ForestChangeDiagnosticDataSpec extends TestEnvironment {

  it ("implicit scope contains correct encoder") {
    val enc = implicitly[ExpressionEncoder[ForestChangeDiagnosticData]]

    // If this test fails likely ExpressionEncoder.apply is being called to derive the encoder through reflection
    // the likely cause is that encoder defined through TypedExpressionEncoder are not present in implicit scope
    withClue(enc.schema.treeString) {
      forAll(enc.schema.fields) { field => field.dataType shouldBe StringType }
    }
  }

  it("creates empty DataFrame") {
    import spark.implicits._
    val data = List(ForestChangeDiagnosticData.empty)
    val df = data.toDF
    df.show()
  }
}
