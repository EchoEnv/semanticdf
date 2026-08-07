package io.semanticdf

import io.semanticdf.core.expr.Expr
import io.semanticdf.core.model._
import io.semanticdf.core.rel.{AggregateCall, AggregateFn, JoinKind}

/** Engine-portable partial-bridge from the legacy [SemanticTable]
  * (spark-flavored) to the engine-portable [Model] (core).
  *
  * Per the multi-engine design \u00a76.4 + PR #408 review: the MCP
  * engine registry currently passes a synthetic [Model] with
  * only the name + a synthetic source (per PR #404). The engine
  * provider ignores the body and uses the legacy [SemanticTable]
  * registry for everything that matters.
  *
  * This bridge produces a REAL [Model] from a [SemanticTable]:
  * dimensions, measures, joins are converted. The engine provider
  * can then consume a real [Model] body, setting up PR #409 (the
  * SparkEngine implements `Engine[R]` work).
  *
  * ==Per-field conversion (v1)==
  *
  * - `name`            <- `SemanticTable.name`
  * - `source`          <- `SemanticTable.sourceTable` (or fallback to name)
  * - `dimensions`      <- `SemanticTable.dimensions` (each as `Expr.FieldRef(name)`)
  * - `measures`        <- `SemanticTable.measures` (each as `AggregateCall(Sum, FieldRef(name), name)`)
  * - `joins`           <- `SemanticTable.joins` (each JoinInfo as a JoinSpec)
  * - `description`     <- `SemanticTable.description`
  * - `version`         <- `SemanticTable.version`
  * - `status`          <- `SemanticTable.status`
  * - `filters`         <- NOT converted (Predicate type duplication; deferred to v0.5.0)
  * - `calculatedMeasures` <- NOT converted (CalcGraph is spark-flavored; deferred to v0.5.0)
  * - `rollups`         <- NOT converted (RollupSpec is engine-portable but the rollup
  *                        definitions live in spark-flavored types; deferred to v0.5.0)
  * - `defaultPolicies` <- `ModelPolicyDefaults.none` (no policy extraction yet)
  * - `extensions`      <- empty (no extension extraction yet)
  *
  * ==Known limitations (v1)==
  *
  * 1. **Measure aggregate function is placeholder `Sum`.** The
  *    legacy `Measure.expr: SemanticScope => Column` is a closure
  *    over a Spark column; we cannot statically determine the
  *    aggregate function (`Sum`, `Avg`, `Count`, etc.) without
  *    evaluating the closure against a sample scope. For v1, we
  *    use `Sum` as a placeholder. Future work (v0.5.0): evaluate
  *    the closure against a synthetic scope to detect the
  *    aggregate function, OR introspect the [Measure.exprString]
  *    hint that may carry the original aggregate expression.
  *
  * 2. **Dimensions are simplified to `Expr.FieldRef(name)`.** The
  *    legacy `Dimension.expr: SemanticScope => Column` may
  *    produce any expression. For v1, we assume the common case
  *    (the dimension name IS the source-column reference; per the
  *    `SemanticTable`'s "name doubles as source-column reference"
  *    invariant). For dimensions whose expr aliases a different
  *    source column, the converted Model will be incorrect.
  *
  * 3. **Join `kind` is mapped from `JoinInfo.cardinality`
  *    (String).** `cardinality = "one" | "many" | "cross"`
  *    maps to `JoinKind.Inner | Inner | Cross`. Inner is the
  *    conservative default; the source code may carry more
  *    granular kind information that is lost in the JoinInfo DTO.
  *
  * 4. **Filters (Predicate) are dropped.** The legacy Predicate
  *    type is spark-flavored; the engine-portable Predicate is a
  *    separate, parallel type. A converter is required; deferred
  *    to v0.5.0.
  *
  * 5. **No policy extraction.** `defaultPolicies` defaults to
  *    `ModelPolicyDefaults.none`. The legacy SemanticTable does
  *    not carry policy information in a structured way.
  *
  * ==Why this lives in semanticdf-spark==
  *
  * The bridge needs BOTH types (spark's SemanticTable + core's
  * Model). `semanticdf-core` deliberately does NOT depend on
  * Spark (the design's "engine-portable" boundary). `semanticdf-spark`
  * is the only module that has both on the classpath.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data conversion (no behavior on the result)
  * - Deterministic: same SemanticTable -> same Model
  * - Total over the easy fields; partial over the hard ones
  *   (documented above)
  */
object ModelBridge {

  /** Map the legacy [io.semanticdf.ModelStatus] (spark adapter)
    * to the engine-portable [io.semanticdf.core.model.ModelStatus].
    *
    * Both ADTs have the same 3 cases (Draft, Published, Deprecated),
    * so the mapping is mechanical. */
  private def statusToCore(s: io.semanticdf.ModelStatus): io.semanticdf.core.model.ModelStatus =
    s match {
      case io.semanticdf.ModelStatus.Draft      => io.semanticdf.core.model.ModelStatus.Draft
      case io.semanticdf.ModelStatus.Published  => io.semanticdf.core.model.ModelStatus.Published
      case io.semanticdf.ModelStatus.Deprecated => io.semanticdf.core.model.ModelStatus.Deprecated
    }

  /** Convert a [SemanticTable] to an engine-portable [Model].
    *
    * Returns `Left(ModelValidationError)` if the converted model
    * fails validation (e.g. duplicate dimension/measure names).
    * Returns `Right(Model)` on success. */
  def toModel(st: SemanticTable): Either[ModelValidationError, Model] = {
    // v1: name resolution priority:
    //   1. SemanticTable.name (the user-declared name from YAML `name:`)
    //   2. sourceTable (the YAML `table:` field, often the same as name)
    //   3. For joined tables where neither is set, derive from the
    //      left side: "<leftName>_<joinType>_<rightName>"
    val name: String = st.name.orElse(st.sourceTable).getOrElse(deriveName(st))
    val source: SourceRef = st.sourceTable match {
      case Some(t) => SourceRef.ByName(catalog = None, namespace = None, table = t)
      case None    => SourceRef.ByName(catalog = None, namespace = None, table = name)
    }
    val dimensions: List[Dimension] = st.dimensions.values.toList.map { d =>
      Dimension(
        name     = d.name,
        expr     = Expr.FieldRef(d.name),
        dataType = None,
      )
    }
    val measures: List[Measure] = st.measures.values.toList.map { m =>
      // v1 placeholder: Sum. See scaladoc for the limitation.
      Measure(
        name = m.name,
        expr = AggregateCall(
          fn    = AggregateFn.Sum,
          input = Some(Expr.FieldRef(m.name)),
          alias = m.name,
        ),
      )
    }
    val joins: List[JoinSpec] = st.joins.toList.map { ji =>
      JoinSpec(
        name       = ji.rightName.getOrElse(""),
        rightModel = ji.rightName.getOrElse(""),
        kind       = cardinalityToJoinKind(ji.cardinality),
        keys       = ji.keys.toList.map(k => k -> k),
      )
    }
    Model.of(
      name               = name,
      source             = source,
      dimensions         = dimensions,
      measures           = measures,
      calculatedMeasures = Nil,
      joins              = joins,
      filters            = Nil,
      rollups            = Nil,
      defaultPolicies    = ModelPolicyDefaults.none,
      extensions         = Map.empty,
      description        = st.description,
      version             = st.version,
      status              = statusToCore(st.status),
    )
  }

  private def cardinalityToJoinKind(cardinality: String): JoinKind =
    cardinality.toLowerCase match {
      case "cross" => JoinKind.Cross
      case _       => JoinKind.Inner  // "one" | "many" | unknown -> Inner
    }

  /** Derive a fallback name for tables without an explicit name or
    * sourceTable (typically joined tables). Returns a synthetic
    * "<leftName>_<rightName>" string; falls back to "unnamed" if
    * the join info is empty. */
  private def deriveName(st: SemanticTable): String = {
    st.joins.headOption.flatMap { ji =>
      ji.leftName.orElse(ji.rightName).map(n => s"${ji.leftName.getOrElse(n)}_${ji.rightName.getOrElse("?")}")
    }.getOrElse("unnamed")
  }
}