package io.semanticdf

/** A row-level filter declared on a model via YAML `filters:` block.
  *
  * Read-only value type. The library maintains these internally as op-tree
  * entries (`SemanticRowFilterOp`), and exposes them through this type for
  * catalog consumers: MCP `describe_model.filters` and OKF `# Filters`.
  */
final case class SemanticFilter(
    name: String,
    description: Option[String],
    expr: String,
    metadata: Map[String, String],
)
