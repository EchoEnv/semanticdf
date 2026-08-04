package io.semanticdf.core.model

/** Engine-portable source identity ADT —
  * Phase 2 contract. Mirrors the design doc §4.1 "SourceRef".
  *
  * `SourceRef` is a SERIALIZABLE source identity. It is deliberately
  * separate from the engine-specific closure that actually produces
  * the source data (e.g. Spark's `() => DataFrame`). The closure
  * lives behind a `ProviderRef` (which references the closure by
  * name, not by value) and is registered once at server startup.
  *
  * ==Why this design==
  *
  * Per the design doc §4.1:
  * > "SourceRef — Serializable source identity, never a native
  * > source object. The closure behind a ProviderRef is driver-
  * > local, never serialized across executors."
  *
  * The portable model holds a `SourceRef` (data — wire-stable, can
  * be sent to any engine). The engine-specific resolver (e.g. a
  * Trino source resolver that maps a `SourceRef.ByName` to a
  * Trino `TableHandle`) consumes the `SourceRef` and produces
  * a `ResolvedSource` (a closed ADT, also engine-portable).
  *
  * ==Why three variants==
  *
  * - `ByName(catalog, namespace, table)` — the model refers to a
  *   table by its name in a catalog. Trino: `hive.sales.orders`.
  *   Spark: `catalog.schema.table`. Databricks: `catalog.schema.table`.
  *   This is the common case.
  * - `ByPath(format, path, options)` — the model refers to a file
  *   on object storage (CSV on S3, Parquet on local disk, etc.).
  *   Useful for batch data lakes.
  * - `ByProvider(provider)` — the model refers to a registered
  *   `() => DataFrame` (or equivalent) provider, by name. This
  *   is the closure-bypass path: a portable model can hold a
  *   `SourceRef.ByProvider` without serializing the actual closure.
  *
  * ==Why a sealed trait and not a string==
  *
  * The design doc §0 correction 5: "There is no `kind: String`."
  * A closed ADT forces every engine adapter to handle the closed
  * set of source-reference variants in code. A free-form `kind: String`
  * field would let adapters invent new variants that the MCP
  * wire format and engine resolvers couldn't classify.
  *
  * ==Why core (engine-portable)==
  *
  * The source identity is the contract that flows through the
  * portable model, through the MCP wire format, and through every
  * engine adapter. The data lives in core; the resolution
  * (mapping a `SourceRef` to an actual `DataFrame` or
  * `TableHandle`) lives in the engine adapter layer.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 3 case classes (no behavior)
  * - Equality auto-derived (case classes)
  * - `Product with Serializable` (Java-serialization round-trip)
  *
  * ==Boundary contract==
  *
  * This file compiles with zero `org.apache.spark.*` imports.
  * Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/SourceRef.scala`
  */
sealed trait SourceRef extends Product with Serializable

object SourceRef {

  /** Reference by table name in a catalog. The `catalog` and
    * `namespace` fields are optional (Trino, Databricks, etc.
    * often have both; some engines use only a flat namespace).
    *
    * Examples:
    *   - `ByName(Some("hive"), Some("sales"), "orders")` →
    *     Trino: `hive.sales.orders`, Spark: `hive.sales.orders`
    *   - `ByName(None, None, "orders")` → engine-default catalog
    *     and namespace, table `orders`
    *
    * Wire-stable: the field name is a contract (MCP `describe_model`,
    * OKF generation, manifest v2). Renaming the case-class fields
    * is a breaking change. */
  final case class ByName(
      catalog:   Option[String],
      namespace: Option[String],
      table:     String,
  ) extends SourceRef

  /** Reference by a file path on object storage or local disk.
    *
    * `format` is the file format (e.g. "csv", "parquet", "json",
    * "orc", "avro"). `path` is the URI (s3://, gs://, abfs://, file://).
    * `options` carries format-specific options (e.g. `header=true`
    * for CSV, `compression=codec` for Parquet). */
  final case class ByPath(
      format:  String,
      path:    String,
      options: Map[String, String] = Map.empty,
  ) extends SourceRef

  /** Reference to a registered `() => DataFrame` (or equivalent)
    * provider, by `ProviderRef`. The provider itself is registered
    * once at server startup and lives in the driver-local registry
    * — it's never serialized across the wire (per the design doc
    * §4.1: "The closure behind a ProviderRef is driver-local, never
    * serialized across executors").
    *
    * This is the closure-bypass path: a portable model can hold
    * a `SourceRef.ByProvider(provider)` without dragging the actual
    * closure through serialization. The engine resolver looks up
    * the provider by `provider.name` at execution time. */
  final case class ByProvider(provider: ProviderRef) extends SourceRef
}