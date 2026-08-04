package io.semanticdf

import org.apache.spark.sql.catalyst.expressions.Expression

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser


/** Validates that a Spark SQL expression references only columns that are
  * visible at the point of evaluation.
  *
  * Sibling of [[SparkFilterValidator]] (which is purpose-built for the
  * `filters:` block — boolean predicates, pre-join semantics). This validator
  * is more general: it's used for `dimensions:`, `transforms:`, and `measures:`
  * expressions, which can be any Spark SQL expression (arithmetic, `case when`,
  * `datediff`, window functions, etc.).
  *
  * ==Why==
  *
  * Without this, a typo in a dimension's SQL expression (e.g.
  * `case when carrrier in (...) then ...` instead of `carrier`) loads silently
  * and surfaces only when the dimension is first materialized in a query, as a
  * cryptic Spark `UNRESOLVED_COLUMN.WITH_SUGGESTION` error. We catch it at
  * model-load time instead.
  *
  * ==Implementation==
  *
  * As of R2 (internal refactor), the parse + column-walk logic lives in
  * [[CatalystColumnValidator]]; this object is now a thin subclass that
  * supplies the per-block error wording and a small wrapper for the
  * public `validate(...)` signature.
  */
private[semanticdf] object ExpressionValidator
    extends CatalystColumnValidator[(String, String, String)] {

  /** Throws `IllegalArgumentException` if the expression references any
    * column not in `visibleColumns`.
    *
    * @param expr            the Spark SQL expression from the YAML `expr:` field
    * @param visibleColumns  columns visible at this point (source + earlier transforms)
    * @param kind            short label for the YAML block: "dimension", "transform", "measure"
    * @param modelName       the model name (for error messages)
    * @param fieldName       the entry's name within the block (for error messages)
    */
  def validate(
      expr: String,
      visibleColumns: Set[String],
      kind: String,
      modelName: String,
      fieldName: String,
  ): Unit = {
    validateColumns(expr, visibleColumns, (kind, modelName, fieldName))
  }

  // ----- CatalystColumnValidator hooks -----

  protected def parseFailure(
      context: (String, String, String),
      expression: String,
      cause: Exception,
  ): IllegalArgumentException = {
    val (kind, modelName, fieldName) = context
    new IllegalArgumentException(
      s"$kind '$fieldName' on model '$modelName': failed to parse Spark SQL " +
      s"expression '$expression': ${cause.getMessage}",
      cause)
  }

  protected def missingColumns(
      context: (String, String, String),
      missing: Set[String],
      visible: Set[String],
      parsed: String,
  ): IllegalArgumentException = {
    val (kind, modelName, fieldName) = context
    new IllegalArgumentException(
      s"$kind '$fieldName' on model '$modelName' references column(s) " +
      s"${missing.toSeq.sorted.map("\"" + _ + "\"").mkString(", ")} that are not " +
      s"visible at this point. Visible columns: " +
      s"${visible.toSeq.sorted.map("\"" + _ + "\"").mkString(", ")}. " +
      s"Did you misspell a column name? For derived columns, declare them in " +
      s"`transforms:` first (transforms run before measures and are visible to them)."
    )
  }
}
