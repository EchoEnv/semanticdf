package io.semanticdf

import org.apache.spark.sql.catalyst.expressions.Expression

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser


/** Validates a Spark SQL filter expression intended for [[SemanticRowFilterOp]].
  *
  * The rule: the expression's column references must all be visible in the
  * source DataFrame's columns. Joins haven't run yet at filter time — columns
  * from joined models are NOT visible to a row filter.
  *
  * Used by the YamlLoader to fail fast (at model-load time) instead of letting
  * a misconfigured filter explode deep inside a query.
  *
  * As of R2 (internal refactor), the parse + column-walk logic lives in
  * [[CatalystColumnValidator]]; this object is now a thin subclass that
  * supplies the per-filter error wording and a small wrapper for the
  * public `validate(...)` signature.
  */
private[semanticdf] object SparkFilterValidator
    extends CatalystColumnValidator[(String, String)] {

  /** Throws `IllegalArgumentException` on parse failure or column-availability failure.
    *
    * @param expr          the Spark SQL filter expression from the YAML `expr:` field
    * @param sourceColumns the columns of THIS model's source DataFrame
    * @param modelName     the model name (for error messages)
    * @param filterName    the filter entry's name (for error messages)
    */
  def validate(
      expr: String,
      sourceColumns: Set[String],
      modelName: String,
      filterName: String,
  ): Unit = {
    validateColumns(expr, sourceColumns, (modelName, filterName))
  }

  // ----- CatalystColumnValidator hooks -----

  protected def parseFailure(
      context: (String, String),
      expression: String,
      cause: Exception,
  ): IllegalArgumentException = {
    val (modelName, filterName) = context
    new IllegalArgumentException(
      s"Filter '$filterName' on model '$modelName': failed to parse Spark SQL " +
      s"expression '$expression': ${cause.getMessage}",
      cause)
  }

  protected def missingColumns(
      context: (String, String),
      missing: Set[String],
      visible: Set[String],
      parsed: String,
  ): IllegalArgumentException = {
    val (modelName, filterName) = context
    new IllegalArgumentException(
      s"Filter '$filterName' on model '$modelName' references column(s) " +
      s"${missing.toSeq.sorted.map("\"" + _ + "\"").mkString(", ")} that are not " +
      s"present on this model's source table. Filters are pre-join: only the " +
      s"model's own source columns are visible at filter time. Apply cross-table " +
      s"predicates via query-time .where(...) instead."
    )
  }
}
