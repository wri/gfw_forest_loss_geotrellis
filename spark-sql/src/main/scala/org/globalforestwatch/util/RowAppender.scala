// package org.globalforestwatch.util

// import org.apache.spark.sql.{Encoder, Row}
// import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

// object RowAppender {
//   def extendRowSchema[T: Encoder](row: Row, name: String) = {
//     val fieldToAdd = {
//       val base = implicitly[Encoder[T]].schema
//       if (base.length == 1)
//         StructField(name, base(0).dataType, base(0).nullable)
//       else
//         StructField(name, base, true)
//     }

//     StructType(row.schema.fields :+ fieldToAdd)
//   }

//   def apply[T: Encoder](row: Row, obj: T, name: String): Row = {
//     val schema = extendRowSchema[T](row, name)

//     new GenericRowWithSchema(row.toSeq.toArray :+ obj, schema)
//   }

//   def addNull[T: Encoder](row: Row, name: String): Row = {
//     val schema = extendRowSchema[T](row, name)

//     new GenericRowWithSchema(row.toSeq.toArray :+ null, schema)
//   }
// }
