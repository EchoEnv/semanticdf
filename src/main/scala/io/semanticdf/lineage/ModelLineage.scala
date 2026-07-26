package io.semanticdf.lineage

import io.semanticdf.ModelStatus

/** The lineage data model — 5 case classes + 2 sealed ADTs.
  *
  * This file is the canonical schema. The JSON wire format (see
  * [[Lineage.toJson]] / [[Lineage.fromJson]]) round-trips through these
  * case classes field-by-field; adding a field here adds a JSON field,
  * removing one is a breaking change.
  *
  * No behavior, no I/O, no SQL. Just data. The transforms live in
  * [[Lineage]]; the SQL parsing lives in [[ColumnRefExtractor]].
  */

/** How much of a field's lineage we could resolve. */
sealed trait LineageStatus
object LineageStatus {
  /** Every base column + every measure-dependency was identified. */
  case object Complete extends LineageStatus

  /** Some references were identified, but one or more couldn't be
    * resolved (e.g. a measure-name reference in the SQL that doesn't
    * match any known measure in the model). */
  case object Partial extends LineageStatus

  /** No `exprString` was available — the field was built from a raw
    * Scala lambda with no source form. We can't recover the base
    * columns or measure-dependencies from the compiled lambda. */
  case object Opaque extends LineageStatus

  def valueOf(s: String): LineageStatus = s match {
    case "Complete" => Complete
    case "Partial"  => Partial
    case "Opaque"   => Opaque
    case other      => throw new IllegalArgumentException(s"Unknown LineageStatus: $other")
  }
}

/** The kind of source this model's data comes from. */
sealed trait SourceKind
object SourceKind {
  case object Batch     extends SourceKind
  case object Streaming extends SourceKind

  def valueOf(s: String): SourceKind = s match {
    case "Batch"     => Batch
    case "Streaming" => Streaming
    case other       => throw new IllegalArgumentException(s"Unknown SourceKind: $other")
  }
}

/** What kind of field this column-lineage entry represents. */
sealed trait ColumnKind
object ColumnKind {
  case object Dimension extends ColumnKind
  case object Measure   extends ColumnKind
  case object Transform extends ColumnKind

  def valueOf(s: String): ColumnKind = s match {
    case "Dimension" => Dimension
    case "Measure"   => Measure
    case "Transform" => Transform
    case other       => throw new IllegalArgumentException(s"Unknown ColumnKind: $other")
  }
}

/** Per-field lineage: which base columns feed this field, which other
  * fields in the same model it depends on (for calc measures), and how
  * much of that we could resolve. */
final case class ColumnLineage(
  name:        String,
  kind:        ColumnKind,
  /** The actual base columns this field reads, in the order they appear
    * in the source. Case-preserved and qualifier-preserved (e.g. `"Customers.OrderDate"`,
    * not `"orderdate"`). Empty for `Opaque` fields. */
  baseColumns: Seq[String]  = Seq.empty,
  /** Other field names in the SAME model that this field references
    * (used for calc measures — e.g. `pct_of_total` depends on `total`).
    * Empty for non-calc fields, or for calc fields whose `exprString`
    * is `Opaque`. */
  dependsOn:   Seq[String]  = Seq.empty,
  /** The SQL form the field was declared with, if available. `None` for
    * fields built from a raw Scala lambda with no source string. */
  exprString:  Option[String] = None,
  status:      LineageStatus,
)

/** One join between two upstream models. */
final case class JoinLineage(
  /** The modelId of the left side, or `"Unknown"` when the model isn't
    * registered in a workspace (i.e. from [[Lineage.of]] in isolation). */
  leftModel:   String,
  /** The modelId of the right side, or `"Unknown"`. */
  rightModel:  String,
  /** Equi-join key pairs in the order they appear in the join. For
    * non-equi joins, this is empty (use `onExprString` on the manifest
    * for the SQL form). */
  keys:        Seq[(String, String)],
  /** `"one"`, `"many"`, or `"cross"` — string not enum so it serializes
    * cleanly to JSON without a sealed-trait encoder. */
  cardinality: String,
)

/** Static-analysis lineage for a single model. */
final case class ModelLineage(
  /** Identity. See `docs/design/lineage.md` §"Model identity" for the
    * MVP limitations — uses `name` for now. */
  modelId:        String,
  /** The model's declared name, if set. May equal `modelId`. */
  modelName:      String,
  /** The source table name (from `SemanticTable.sourceTable`), or
    * `None` for Scala-DSL-built models that didn't declare one.
    * Consumers interpret `None` as `"Unknown"`. */
  sourceTable:    Option[String]        = None,
  sourceKind:     SourceKind            = SourceKind.Batch,
  status:         ModelStatus,
  dimensions:     Seq[ColumnLineage]   = Seq.empty,
  measures:       Seq[ColumnLineage]   = Seq.empty,
  transforms:     Seq[ColumnLineage]   = Seq.empty,
  /** One entry per join in the model. Empty for single-table models. */
  joins:          Seq[JoinLineage]      = Seq.empty,
  /** ModelIds this model depends on via joins. Populated by
    * [[Lineage.workspaceOf]] from the workspace map; empty when
    * [[Lineage.of]] is called in isolation. */
  upstreamModels: Seq[String]           = Seq.empty,
)

/** The whole lineage graph for a workspace. */
final case class WorkspaceLineage(
  /** modelId -> lineage. */
  models:        Map[String, ModelLineage],
  /** Pre-computed reverse-lookup: `upstreamOf(modelId)` = the set of
    * modelIds that depend on `modelId` via joins. */
  upstreamOf:    Map[String, Set[String]],
  /** Pre-computed reverse-lookup: `downstreamOf(modelId)` = the set of
    * modelIds that `modelId` depends on via joins. */
  downstreamOf:  Map[String, Set[String]],
)
