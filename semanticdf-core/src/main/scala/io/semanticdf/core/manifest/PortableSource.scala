package io.semanticdf.core.manifest

/** Engine-portable source reference — YAML shape (v1).
  *
  * Per the v0.3.2 design doc: portable reader intermediate. The
  * reader (3A.2) converts this to `core.model.SourceRef`.
  *
  * Mirrors the `core.model.SourceRef` shape but holds raw
  * strings (the reader maps to the typed variants).
  *
  * ==Why a sealed trait (vs. a single case class with all fields)==
  *
  * Per scala-data-driven-refacer §3 ("default: sealed trait +
  * match"): 3 mutually-exclusive source shapes (ByName / ByPath /
  * ByProvider). Sealed trait + match forces exhaustive handling
  * at the reader (3A.2). A single case class would let callers
  * mix-and-match invalid combinations.
  *
  * ==Why `path` and `format` are Option==
  *
  * `ByName` doesn't have a path (it references a catalog table).
  * `ByPath` doesn't have a catalog/namespace (it's a file URI).
  * `ByProvider` doesn't have either (it references a provider).
  * Optional fields + sealed trait = pattern-match enforcement.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 3 case classes (no behavior)
  * - `extends Product with Serializable` on the trait and each case
  * - Each variant has only the fields it needs */
sealed trait PortableSource extends Product with Serializable

object PortableSource {

  /** Reference by table name in a catalog.
    *
    * Examples:
    *   - `{type: "ByName", catalog: "hive", namespace: "sales", table: "orders"}`
    *   - `{type: "ByName", catalog: null, namespace: null, table: "orders"}` */
  final case class ByName(
      catalog:   Option[String],
      namespace: Option[String],
      table:     String,
  ) extends PortableSource

  /** Reference by a file path.
    *
    * Examples:
    *   - `{type: "ByPath", path: "s3://bucket/orders.parquet", format: "parquet"}`
    *   - `{type: "ByPath", path: "/tmp/orders.csv", format: "csv", options: {header: "true"}}` */
  final case class ByPath(
      path:    String,
      format:  String,
      options: Map[String, String] = Map.empty,
  ) extends PortableSource

  /** Reference by a provider (e.g. Iceberg table, Kafka topic).
    *
    * `provider` is the provider name (e.g. "iceberg", "kafka",
    * "delta"). `identifier` is the provider-specific handle.
    *
    * Examples:
    *   - `{type: "ByProvider", provider: "iceberg", identifier: "orders"}` */
  final case class ByProvider(
      provider:   String,
      identifier: String,
  ) extends PortableSource
}
