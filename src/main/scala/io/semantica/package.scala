package io

import org.apache.spark.sql.DataFrame

/** Semantica — a semantic layer for Apache Spark (JVM).
  *
  * This package object is the public entry point (DESIGN §4.3). `import io.semantica._`
  * brings [[toSemanticTable]] into scope.
  */
package object semantica {

  /** Construct a [[SemanticTable]] from a base DataFrame — the primary entry point.
    *
    * Mirrors BSL's `to_semantic_table`. The `table` may be a batch DataFrame today;
    * the construction surface is identical for a streaming source once the streaming
    * terminal lands (ADR 0002).
    *
    * @param table       the base DataFrame
    * @param name        optional model name (used later for join prefixing)
    * @param description optional human-readable description
    */
  def toSemanticTable(
      table: DataFrame,
      name: Option[String] = None,
      description: Option[String] = None,
  ): SemanticTable =
    new SemanticTable(SemanticTableOp(table, name, description))
}
