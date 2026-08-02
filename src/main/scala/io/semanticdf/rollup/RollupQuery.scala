package io.semanticdf.rollup

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit


import io.semanticdf.{SemanticTable, SortKey}
import io.semanticdf.predicate.Predicate

/** A query that runs against a pre-aggregated [[Rollup]] rather than
  * the raw fact table.
  *
  * This is the v0.2.4 REDESIGN of the manual rollups feature.
  *
  * ## Why a separate type
  *
  * The previous design (v0.2.4 PR #328 / #329) made `useRollup` return
  * a `SemanticTable` with `root = SemanticRollupOp(...)`. This intertwined
  * the rollup state with the existing op tree and caused 5 audit cycles
  * with 19+ HIGH-severity bugs -- `MatchError` on joins, `IllegalStateException`
  * on `withDimensions`, silent wrong data on `groupBy().aggregate()`, etc.
  *
  * The new design returns a separate `RollupQuery` type that:
  *   1. Has NO method `withDimensions`, `withMeasures`, `groupBy`, `aggregate`,
  *      `join_one`, `atTimeGrain`, etc. -- so the user CAN'T call them
  *      on a rollup query at all (compile error).
  *   2. Has only the operations that make sense for a rollup: filter
  *      (`withWhere`), order (`withOrderBy`), limit (`withLimit`), execute.
  *   3. Doesn't pollute `SemanticTable` with a `rollups` field.
  *   4. Holds the [[RollupRegistry]] directly (not on the SemanticTable).
  *
  * This means the type system prevents the bugs at compile time, not
  * runtime.
  *
  * ## Usage
  *
  * {{{
  *   val model = orders.withRollup(rollup)
  *   val registry = RollupRegistry.empty.register("orders_by_rc", _ => spark.read.parquet(path))
  *   val result = model.useRollup("orders_by_rc", registry)
  *     .withWhere(predicate)
  *     .withOrderBy(SortKey.desc("total"))
  *     .withLimit(10)
  *     .execute(spark)
  * }}}
  *
  * @param model    the originating [[SemanticTable]] (for lineage; not
  *                 modified)
  * @param rollup   the pre-aggregated rollup
  * @param registry runtime container holding the DataFrame provider
  * @param where    optional WHERE clause to apply (parsed to a Column
  *                 at execute time)
  * @param orderBy  optional ORDER BY keys
  * @param limit    optional LIMIT
  */
final class RollupQuery private[rollup] (
  model:    SemanticTable,
  rollup:   Rollup,
  registry: RollupRegistry,
  where:    Option[Predicate]    = None,
  orderBy:  Seq[SortKey]         = Nil,
  limit:    Option[Int]          = None,
) {
  /** The originating [[SemanticTable]]. Useful for lineage / debugging. */
  def sourceModel: SemanticTable = model

  /** The rollup being used. */
  def rollupUsed: Rollup = rollup

  /** Add a WHERE clause. The predicate's columns must all be in the
    * rollup source (validated at execute time -- parse don't validate).
    *
    * If the predicate references columns not in the rollup source, the
    * execute() will throw with a clear error.
    */
  def withWhere(predicate: Predicate): RollupQuery =
    new RollupQuery(model, rollup, registry, Some(predicate), orderBy, limit)

  /** Add an ORDER BY clause. */
  def withOrderBy(keys: SortKey*): RollupQuery =
    new RollupQuery(model, rollup, registry, where, keys.toSeq, limit)

  /** Add a LIMIT clause. */
  def withLimit(n: Int): RollupQuery =
    new RollupQuery(model, rollup, registry, where, orderBy, Some(n))

  /** Execute the query against the rollup source. Returns the
    * pre-aggregated DataFrame projected to the user's requested
    * dimensions + measures.
    */
  def execute(spark: SparkSession): DataFrame = {
    val source = registry.loadSource(rollup.name)
      .getOrElse(throw new IllegalStateException(
        s"Rollup '${rollup.name}' not registered in the supplied RollupRegistry. " +
        s"Use `RollupRegistry.register(name, provider)` to add it before executing."
      ))

    // 1. Apply WHERE if set (against source schema, BEFORE projection)
    val filtered = where.fold(source) { pred =>
      // Validate predicate columns exist in the SOURCE schema
      // (rollup source may have columns not in the dim+measure projection,
      // e.g., a date column the user wants to filter on).
      val predCols = pred.fields
      val sourceCols = source.schema.fieldNames.toSet
      val missing = predCols -- sourceCols
      if (missing.nonEmpty) {
        throw new IllegalStateException(
          s"Rollup '${rollup.name}' WHERE clause references columns " +
          s"$missing that aren't in the rollup source. " +
          s"Available: ${source.schema.fieldNames.toList.sorted}"
        )
      }
      // Convert predicate to a Column. Schema lookup is implicit via
      // Spark's `col(name)` which resolves against the DataFrame at
      // apply-time — no explicit `schema` parameter is needed.
      val col = predicateToColumn(pred)
      source.where(col)
    }

    // 2. Project: dimensions + storage columns (renamed to measure names)
    val dimCols     = rollup.rollupDimensions.map(col)
    val measureCols = rollup.rollupMeasures.map { m =>
      col(m.storageCol).as(m.name)
    }
    val projection = (dimCols ++ measureCols).distinct
    val projected  = filtered.select(projection: _*)

    // 3. Reassign for downstream ORDER BY / LIMIT (no longer mutating)
    val withWhere = projected

    // 3. Apply ORDER BY if set
    val withOrder = if (orderBy.isEmpty) withWhere else withWhere.orderBy(orderBy.map(_.toColumn): _*)

    // 4. Apply LIMIT if set
    limit.fold(withOrder)(n => withOrder.limit(n))
  }

  /** Convert a [[Predicate]] to a Spark [[Column]].
    *
    * Recursive on [[io.semanticdf.predicate.Predicate.And]] /
    * [[io.semanticdf.predicate.Predicate.Or]] /
    * [[io.semanticdf.predicate.Predicate.Not]]; flat on every other
    * [[Predicate]] subtype.
    *
    * All known [[Predicate]] subtypes are matched (the [[Predicate]] ADT
    * is sealed). The catch-all is defensive code for the case where a
    * future subtype is added to `Predicate` without updating this
    * method — at which point a `scala.MatchError` would surface in
    * production. The message hints at which file to update.
    */
  private def predicateToColumn(pred: Predicate): Column = {
    val fieldCol: String => org.apache.spark.sql.Column = org.apache.spark.sql.functions.col
    pred match {
      case io.semanticdf.predicate.Predicate.Compare.Eq(field, value)  => fieldCol(field) === value
      case io.semanticdf.predicate.Predicate.Compare.Ne(field, value)  => fieldCol(field) =!= value
      case io.semanticdf.predicate.Predicate.Compare.Lt(field, value)  => fieldCol(field) < value
      case io.semanticdf.predicate.Predicate.Compare.Le(field, value)  => fieldCol(field) <= value
      case io.semanticdf.predicate.Predicate.Compare.Gt(field, value)  => fieldCol(field) > value
      case io.semanticdf.predicate.Predicate.Compare.Ge(field, value)  => fieldCol(field) >= value
      case io.semanticdf.predicate.Predicate.Compare.Contains(field, value) =>
        fieldCol(field).contains(org.apache.spark.sql.functions.lit(value))
      case io.semanticdf.predicate.Predicate.Compare.StartsWith(field, value) =>
        fieldCol(field).startsWith(org.apache.spark.sql.functions.lit(value))
      case io.semanticdf.predicate.Predicate.Compare.EndsWith(field, value) =>
        fieldCol(field).endsWith(org.apache.spark.sql.functions.lit(value))
      case io.semanticdf.predicate.Predicate.Compare.ArrayContains(field, value) =>
        // ArrayContains(field, value) means "field array contains value".
        // Simplest correct form for v0.2.4: use array_contains function.
        org.apache.spark.sql.functions.array_contains(fieldCol(field), org.apache.spark.sql.functions.lit(value))
      case io.semanticdf.predicate.Predicate.And(children @ _*) => children.map(predicateToColumn(_)).reduce(_ && _)
      case io.semanticdf.predicate.Predicate.Or(children @ _*)  => children.map(predicateToColumn(_)).reduce(_ || _)
      case io.semanticdf.predicate.Predicate.Not(child)         => !predicateToColumn(child)
      case io.semanticdf.predicate.Predicate.IsNull(field, false) => fieldCol(field).isNull
      case io.semanticdf.predicate.Predicate.IsNull(field, true)  => fieldCol(field).isNotNull
      case io.semanticdf.predicate.Predicate.In(field, values, false) => fieldCol(field).isin(values.map(org.apache.spark.sql.functions.lit): _*)
      case io.semanticdf.predicate.Predicate.In(field, values, true)  => !fieldCol(field).isin(values.map(org.apache.spark.sql.functions.lit): _*)
      // Defensive: every Predicate subtype is matched above (Predicate is sealed).
      // If a future subtype is added without updating this match, the error
      // here points to the file/method to fix rather than a bare MatchError.
      case _ => throw new IllegalStateException(
        s"Rollup WHERE clause: ${pred.getClass.getSimpleName} is not handled by " +
        s"`RollupQuery.predicateToColumn`. Add a case — see rollup/RollupQuery.scala."
      )
    }
  }
}
