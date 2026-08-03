package io.semanticdf

/** Summary of one join in a semantic model — exposed for MCP `describe_model`.
  *
  * Captures the cardinality, side names, grain (join-key) columns, and any
  * dimensions/measures added via `withDimensions` / `withMeasures` after the
  * join. Internalised `SemanticJoinOp` is kept private to the package; this
  * DTO is the stable, MCP-facing shape. */
final case class JoinInfo(
    /** Cardinality as a string ("one" | "many" | "cross") — string not enum so
      * it serializes cleanly to JSON without a sealed-trait encoder. */
    cardinality: String,
    /** Name of the left-side source model (e.g. "orders"). None if anonymous. */
    leftName: Option[String],
    /** Name of the right-side source model (e.g. "customers"). None if anonymous. */
    rightName: Option[String],
    /** Join-key column names — the equi-join keys. Empty for cross joins. */
    keys: Seq[String],
    /** Names of dimensions added via `withDimensions` after this join. */
    extraDimensions: Seq[String],
    /** Names of measures added via `withMeasures` after this join. */
    extraMeasures: Seq[String],
)
