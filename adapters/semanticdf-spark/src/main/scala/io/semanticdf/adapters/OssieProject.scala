package io.semanticdf.adapters

/** The intermediate form produced by [[OssieReader.parse]].
  *
  * Holds the raw Ossie structure (one or more `semantic_model` entries
  * plus the optional `ontology` concept declarations) without any
  * semanticdf-specific mapping. The `toSemanticTables` step
  * converts the canonical `semantic_model` entries into
  * [[io.semanticdf.SemanticTable]]s; the `ontology` is preserved on
  * the project for downstream consumers but is not used to build
  * the tables (it carries concept-level metadata that has no
  * first-class semanticdf concept today).
  *
  * One `OssieProject` per `semantic_model` entry. The `ontology`
  * map is shared across all projects from the same source file
  * (it's the file-level concept dictionary). */
final case class OssieProject(
    name:        String,
    description: Option[String],
    datasets:    Seq[OssieDataset],
    relationships: Seq[OssieRelationship],
    metrics:     Seq[OssieMetric],
    /** Raw ontology (concept declarations) — preserved for v2
      * consumers; not used to build tables in v1. */
    ontology:    Seq[Map[String, Any]] = Seq.empty,
)

/** One Ossie `dataset` — corresponds to a single fact or dimension
  * table. Maps directly to a semanticdf [[io.semanticdf.SemanticTable]]. */
final case class OssieDataset(
    name:        String,
    source:      String,
    description: Option[String] = None,
    primaryKey:  Seq[String] = Seq.empty,
    uniqueKeys:  Seq[Seq[String]] = Seq.empty,
    fields:      Seq[OssieField] = Seq.empty,
)

/** One Ossie `field` — a column on a dataset. Becomes a semanticdf
  * [[io.semanticdf.Dimension]] (with `is_time` if `dimension.is_time`
  * is true) or a [[io.semanticdf.Measure]] (if the field is a
  * `metric` in the parent's `metrics` list — see [[OssieMetric]]). */
final case class OssieField(
    name:        String,
    /** SQL expression for this field. Picked from the dialect list;
      * ANSI_SQL is the default, fall back to the first dialect. */
    expression:  String,
    description: Option[String] = None,
    /** Logical data type — `String | Integer | Decimal | Float |
      * Boolean | Date | Time | DateTime | DateTimeTz | Opaque`. */
    datatype:    Option[String] = None,
    /** `true` if this field is a time dimension. */
    isTimeDimension: Boolean = false,
)

/** One Ossie `relationship` — corresponds to a semanticdf join.
  * `from` is the many side, `to` is the one side (Ossie's convention). */
final case class OssieRelationship(
    name:         String,
    from:         String,
    to:           String,
    fromColumns:  Seq[String],
    toColumns:    Seq[String],
)

/** One Ossie `metric` — a top-level aggregate defined in the
  * `metrics` array. Becomes a semanticdf [[io.semanticdf.Measure]].
  *
  * `qualifier` is the dataset name the metric was bound to in the
  * original Ossie expression, if any (e.g. `SUM(orders.amount)`
  * has qualifier `Some("orders")`). An unqualified metric
  * (e.g. `COUNT(1)`) has `None`. The OssieReader uses this to
  * decide which dataset the metric belongs to — see the reader's
  * `toSemanticTables` for the resolution rules. */
final case class OssieMetric(
    name:        String,
    expression:  String,
    qualifier:   Option[String] = None,
    description: Option[String] = None,
    datatype:    Option[String] = None,
)
