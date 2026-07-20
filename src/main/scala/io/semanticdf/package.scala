package io

import org.apache.spark.sql.DataFrame

/** SemanticDF — a semantic layer for Apache Spark (JVM).
  *
  * This package object is the public entry point (DESIGN §4.3). `import io.semanticdf._`
  * brings [[toSemanticTable]] into scope.
  */
package object semanticdf {

  // -------------------------------------------------------------------------
  // Shared utilities
  // -------------------------------------------------------------------------

  /** Closest string match to `name` among `candidates`, if within a length-scaled
    * edit-distance threshold.
    *
    * Threshold = `max(3, name.length / 4)`. The fixed floor of 3 covers single-char
    * swaps, insertions, deletions. The length-proportional component scales the
    * threshold for long namespaced measure/dimension names (e.g. a 4-edit typo in a
    * 32-char name would silently miss the suggestion under a fixed threshold).
    * Returns None when no candidate is close enough. */
  def closestMatch(name: String, candidates: Iterable[String]): Option[String] = {
    if (name.isEmpty || candidates.isEmpty) return None
    val best = candidates.map(c => c -> editDistance(name.toLowerCase, c.toLowerCase)).minBy(_._2)
    if (best._2 <= math.max(3, name.length / 4)) Some(best._1) else None
  }

  /** Levenshtein edit distance between two strings. */
  private[semanticdf] def editDistance(a: String, b: String): Int = {
    val n = a.length; val m = b.length
    if (n == 0) return m
    if (m == 0) return n
    val prev = Array.tabulate(m + 1)(identity)
    val curr = new Array[Int](m + 1)
    var i = 1
    while (i <= n) {
      curr(0) = i
      var j = 1
      while (j <= m) {
        curr(j) = math.min(
          math.min(curr(j - 1) + 1, prev(j) + 1),
          prev(j - 1) + (if (a(i - 1) == b(j - 1)) 0 else 1)
        )
        j += 1
      }
      i += 1
      System.arraycopy(curr, 0, prev, 0, m + 1)
    }
    prev(m)
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /** Construct a [[SemanticTable]] from a base DataFrame — the primary entry point.
    *
    * Mirrors BSL's `to_semantic_table`. The `table` may be a batch DataFrame today;
    * the construction surface is identical for a streaming source once the streaming
    * terminal lands (ADR 0002).
    *
    * @param table       the base DataFrame
    * @param name        optional model name (used later for join prefixing)
    * @param description optional human-readable description
    * @param sourceTable optional name of the underlying source (e.g. a Spark table
    *                    name, or a name resolved against [[YamlLoader]]). Propagated
    *                    through every model transformation so MCP `describe_model`
    *                    can report it.
    */
  def toSemanticTable(
      table: DataFrame,
      name: Option[String] = None,
      description: Option[String] = None,
      sourceTable: Option[String] = None,
  ): SemanticTable =
    new SemanticTable(
      SemanticTableOp(table, name, description),
      sourceTable = sourceTable,
    )

  /** Construct a [[SemanticTable]] from a streaming source for use with
    * [[SemanticTable.toStreamingQuery]] (ADR 0002).
    *
    * Parallel to [[toSemanticTable]] but for streaming. The `stream` must
    * come from `spark.readStream` (i.e. `stream.isStreaming` is true).
    * The model construction (`withDimensions`, `withMeasures`, `where`)
    * is otherwise identical to the batch path.
    *
    * Example:
    * {{{
    *   val rate = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    *   val model = toStreamingSemanticTable(rate, name = Some("rate"))
    *     .withDimensions(Dimension("value", t => t("value")))
    *     .withMeasures(Measure("count", t => count(lit(1))))
    *   val q = model.toStreamingQuery(spark, StreamingQueryOptions(
    *     foreachBatch = (df: DataFrame) => df.write.parquet("/tmp/out")))
    *   q.awaitTermination()
    * }}}
    */
  def toStreamingSemanticTable(
      stream: DataFrame,
      name: Option[String] = None,
      description: Option[String] = None,
      sourceTable: Option[String] = None,
  ): SemanticTable = {
    require(stream.isStreaming,
      "toStreamingSemanticTable requires a streaming DataFrame (from spark.readStream)")
    new SemanticTable(
      SemanticStreamingTableOp(stream, name, description),
      sourceTable = sourceTable,
    )
  }
}
