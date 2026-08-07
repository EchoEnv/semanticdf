package io.semanticdf.spark

import io.semanticdf.core.engine.{EngineContext, EngineError, ResolvedSource, SourceResolver}
import io.semanticdf.core.expr.Expr
import io.semanticdf.core.model.{JoinSpec, Model, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn, JoinKind}

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, countDistinct, lit}

/** Engine-specific Spark compiler that walks a portable [[Model]]
  * and emits a Spark [[DataFrame]] (per scala-data-driven-refacer
  * \u00a71: behavior in adapters; the `Model` is pure data in core).
  *
  * ==Why a Spark-native compiler (not SQL emission)==
  *
  * The Trino + DuckDB compilers emit SQL strings; Spark's API is
  * typed (`DataFrame.transform(...)`). Different output shapes;
  * can't share a compiler.
  *
  * ==v0.3.1 scope (Gap 1 partial closure)==
  *
  * Supports: source resolution, filters, joins (single-key Inner /
  * Left / Right / Full / Cross), dimensions (groupBy),
  * measures (Sum / Count / CountDistinct / Avg / Min / Max).
  * NOT in v0.3.1 scope: multi-key joins, calculated measures
  * (Calc-of-calc), `t.all` / `Expr.MeasureRef`, advanced aggregates
  * (Stddev / Variance / Median / Percentile \u2014 deferred per Gap 5
  * for SQL; Spark-side via this compiler later).
  *
  * ==JVM-safety checks==
  *
  * - Check 1 (Null): null `SourceRef` paths return `Left(...)` instead
  *   of NPE.
  * - Check 2 (Resource): the returned `DataFrame` is lazy; the caller
  *   materializes via `.collect()` (which Spark manages).
  * - Check 3 (Long-lived state): no caches; the walk is stateless.
  * - Check 4 (Stack): no recursion \u2014 the walk is iterative over the
  *   flat `Model` fields.
  */
class PortableQueryCompiler {

  /** Compile a portable [[Model]] into a Spark [[DataFrame]].
    *
    * @return `Right(DataFrame)` on success; `Left(EngineError)` if
    *         the source can't be resolved or a join references an
    *         unknown model. */
  def compile(
      model: Model,
      ctx:   EngineContext,
  ): Either[EngineError, DataFrame] = {
    for {
      sourceDf <- resolveSource(model.source)
      filtered  <- Right(applyFilters(sourceDf, model.filters))
      joined    <- applyJoins(filtered, model.joins)
      grouped   <- Right(applyAggregations(joined, model))
    } yield grouped
  }

  // -- source resolution --

  /** Resolve a [[SourceRef]] to a Spark `DataFrame`.
    *
    * v0.3.1 scope: only `SourceRef.ByName` is supported (looks up the
    * table in the active Spark catalog via `spark.table(...)`). Other
    * variants (`ByPath`, `ByProvider`) return `Left(...)` with a clear
    * message.
    *
    * The Spark session is supplied via the implicit `SparkSession`
    * \u2014 in production, the engine injects this; in tests, it's
    * brought into scope by `SparkSessionFixture`. */
  private def resolveSource(
      source: SourceRef,
  ): Either[EngineError, DataFrame] = {
    import PortableQueryCompiler.sparkSession
    source match {
      case SourceRef.ByName(catalog, namespace, table) =>
        try {
          val spark = sparkSession
          // Try fully-qualified first; fall back to bare table name
          // (covers both catalog tables and `createOrReplaceTempView`
          // views that don't have a catalog/namespace).
          val qualified = (catalog, namespace) match {
            case (Some(c), Some(n)) => s"$c.$n.$table"
            case (None,    Some(n)) => s"default.$n.$table"
            case _                  => table
          }
          try {
            Right(spark.table(qualified))
          } catch {
            case _: Exception if qualified != table =>
              // Retry with just the table name (covers temp views).
              try {
                Right(spark.table(table))
              } catch {
                case _: Exception =>
                  Left(EngineError.UnsupportedCapability(
                    name   = "SourceRef.ByName",
                    reason = s"Spark table '$table' (qualified: '$qualified') not found.",
                  ))
              }
            case _: Exception =>
              Left(EngineError.UnsupportedCapability(
                name   = "SourceRef.ByName",
                reason = s"Spark table '$qualified' not found.",
              ))
          }
        } catch {
          case e: Exception =>
            Left(EngineError.UnsupportedCapability(
              name   = "SourceRef.ByName",
              reason = s"Spark table resolution failed: ${e.getMessage}",
            ))
        }
      case SourceRef.ByPath(_, _, _) =>
        Left(EngineError.UnsupportedCapability(
          name   = "SourceRef.ByPath",
          reason = "SourceRef.ByPath is not supported in Spark v0.3.1 (deferred to v0.4.0).",
        ))
      case SourceRef.ByProvider(_) =>
        Left(EngineError.UnsupportedCapability(
          name   = "SourceRef.ByProvider",
          reason = "SourceRef.ByProvider is not supported in Spark v0.3.1 (deferred to v0.4.0).",
        ))
    }
  }

  // -- filter application --

  private def applyFilters(
      df:      DataFrame,
      filters: List[io.semanticdf.core.model.FilterSpec],
  ): DataFrame = filters.foldLeft(df) { (acc, f) =>
    acc.filter(PortableExprCompiler.toColumn(f.predicate))
  }

  // -- join application --

  private def applyJoins(
      df:    DataFrame,
      joins: List[JoinSpec],
  ): Either[EngineError, DataFrame] = {
    import PortableQueryCompiler.sparkSession
    joins.foldLeft[Either[EngineError, DataFrame]](Right(df)) { (accE, js) =>
      accE.flatMap { accDf =>
        val spark = sparkSession
        // Resolve the right-side model by name in the active catalog.
        // v0.3.1 scope: single-key Inner/Left/Right/Full/Cross joins.
        val rightDf = try {
          spark.table(js.rightModel)
        } catch {
          case _: Exception =>
            return Left(EngineError.UnsupportedCapability(
              name   = "JoinSpec.rightModel",
              reason = s"Right-side model '${js.rightModel}' not found. " +
                       s"Multi-key joins deferred to v0.4.0.",
            ))
        }
        // Single-key ON clause (multi-key is v0.4.0 scope).
        if (js.keys.size != 1) {
          return Left(EngineError.UnsupportedCapability(
            name   = "JoinSpec.keys",
            reason = s"Multi-key joins (${js.keys.size} keys) deferred to v0.4.0.",
          ))
        }
        val (leftKey, rightKey) = js.keys.head
        val joinType = js.kind match {
          case JoinKind.Inner => "inner"
          case JoinKind.Left  => "left"
          case JoinKind.Right => "right"
          case JoinKind.Full  => "outer"
          case JoinKind.Cross => "cross"
        }
        Right(accDf.join(rightDf, accDf(leftKey) === rightDf(rightKey), joinType))
      }
    }
  }

  // -- aggregation application --

  private def applyAggregations(
      df:    DataFrame,
      model: Model,
  ): DataFrame = {
    if (model.measures.isEmpty) return df
    // Aggregations
    val aggCols: List[Column] = model.measures.map { m =>
      renderAggregate(m.expr).as(m.name)
    }
    // groupBy on dimensions (if any); else aggregate over the whole df
    val dimCols: Array[Column] = model.dimensions.map { d =>
      PortableExprCompiler.toColumn(d.expr)
    }.toArray
    if (dimCols.isEmpty) {
      // No dimensions → aggregate over the whole df (single-row result).
      df.agg(aggCols.head, aggCols.tail: _*)
    } else {
      df.groupBy(dimCols: _*).agg(aggCols.head, aggCols.tail: _*)
    }
  }

  /** Render a portable `AggregateCall` as a Spark `Column`. */
  private def renderAggregate(call: AggregateCall): Column = {
    import org.apache.spark.sql.functions.{avg, max => sparkMax, min => sparkMin, sum => sparkSum}
    val input = call.input.getOrElse(Expr.FieldRef(call.alias))
    val inputCol = PortableExprCompiler.toColumn(input)
    val fn = call.fn match {
      case AggregateFn.Sum           => sparkSum(inputCol)
      case AggregateFn.Count         => count(lit(1))
      case AggregateFn.CountDistinct => countDistinct(inputCol)
      case AggregateFn.Avg           => avg(inputCol)
      case AggregateFn.Min           => sparkMin(inputCol)
      case AggregateFn.Max           => sparkMax(inputCol)
      case other =>
        throw new UnsupportedOperationException(
          s"PortableQueryCompiler.renderAggregate: $other is not implemented in v0.3.1 " +
          s"(use DuckDBQueryCompiler / TrinoQueryCompiler for SQL-side advanced aggregates).",
        )
    }
    fn
  }
}

/** Spark session carrier. Production sets this via a custom
  * initialization; tests set it via `SparkSessionFixture`'s
  * `beforeAll` hook. The compiler reads it lazily through a
  * mutable ref so the same compiler instance works across both
  * production (one global session) and tests (per-spec session).
  *
  * Per JVM-safety check 3 (long-lived state): this is a single
  * `var` cleared by `clear()`; no concurrency guarantees. */
object PortableQueryCompiler {
  @volatile private var _spark: Option[SparkSession] = None

  /** Set the active Spark session. Production calls this once at
    * engine init; tests call this in `beforeAll`. */
  def setSparkSession(s: SparkSession): Unit = synchronized {
    _spark = Some(s)
  }

  /** Clear the active Spark session. Called by `SparkSessionFixture`'s
    * `afterAll` to ensure the next test starts clean. */
  def clearSparkSession(): Unit = synchronized {
    _spark = None
  }

  /** Get the active Spark session, or fail if not set. The compiler
    * accesses this through this indirection rather than capturing
    * the session in the constructor so it can survive SparkSession
    * lifecycle changes in tests. */
  def sparkSession: SparkSession =
    _spark.getOrElse(throw new IllegalStateException(
      "PortableQueryCompiler: no SparkSession set. Call setSparkSession(...) before compile(...).",
    ))
}
