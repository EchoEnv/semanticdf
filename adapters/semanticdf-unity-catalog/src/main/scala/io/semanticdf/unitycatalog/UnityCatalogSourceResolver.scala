package io.semanticdf.unitycatalog

import io.semanticdf.core.engine.{EngineIdentity, ResolvedSchema, ResolvedSource, SourceResolver}
import io.semanticdf.core.model.SourceRef
import io.semanticdf.core.schema.SealedDataType

/** Engine-specific Unity Catalog source-resolver implementation.
  *
  * Implements the engine-portable `SourceResolver` contract
  * (from `io.semanticdf.core.engine`) using a Unity Catalog-
  * specific `UnityCatalogClient`. This is the **FIRST catalog
  * adapter** in the multi-engine design — the proof that the
  * §4.6 layer-separation principle works.
  *
  * ==What this class does==
  *
  * Resolves portable `SourceRef`s to engine-portable
  * `ResolvedSource` results by calling Unity Catalog:
  *   - `SourceRef.ByName` → `GET /api/2.1/unity-catalog/tables/{full_name}`
  *     (via `UnityCatalogClient.describeTable`)
  *   - `SourceRef.ByPath` → REJECTED (Unity Catalog doesn't have
  *     portable path-based sources — only registered tables)
  *   - `SourceRef.ByProvider` → REJECTED (the `ProviderRef`
  *     mechanism is Spark-specific; Unity Catalog doesn't
  *     support portable DataFrame providers)
  *
  * The resolution result is one of:
  *   - `ResolvedSource.Scan(source, schema)` — successful
  *     resolution (ByName with a known table)
  *   - `ResolvedSource.Incompatible(source, reason)` — the
  *     source's shape isn't supported (ByPath, ByProvider)
  *   - `ResolvedSource.NotFound(source, reason)` — the table
  *     doesn't exist (ByName with an unknown table)
  *
  * Per the design: `ResolvedSource.AuthFailed` is reserved for
  * auth failures, but this implementation can't distinguish
  * auth failures from "table not found" via the simple
  * `describeTable` API. Future PRs that extend
  * `UnityCatalogClient` to surface auth errors distinctly
  * would map `None` to `AuthFailed` when the reason is auth-
  * related. For now, `None` maps to `NotFound`.
  *
  * ==Why an injected `UnityCatalogClient`==
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior
  * lives elsewhere"): the BEHAVIOR (calling UC, parsing JSON)
  * lives here. The `UnityCatalogClient` trait (in the same
  * package) is the boundary that makes this class unit-
  * testable without a real Unity Catalog cluster. Tests inject
  * a `FakeUnityCatalogClient`.
  *
  * ==Why `extends SourceResolver`==
  *
  * The portable contract is in core (`SourceResolver`). This
  * class implements that contract. The `Engine.compile` method
  * can resolve a `Model.source` via this resolver before
  * emitting SQL — the resolver's result feeds into the plan.
  *
  * ==Why the `ByPath` / `ByProvider` cases return `Incompatible`==
  *
  * Per the design's `ProviderRef.DataFrameSource`:
  * "DataFrame-specific provider refs return IncompatibleEngine
  * on non-Spark engines." Similarly, `ByPath` is Spark-centric
  * (Unity Catalog doesn't have a portable path-based source —
  * it has registered tables only). Both cases are explicitly
  * rejected here.
  *
  * ==Why this resolver is engine-portable too==
  *
  * Per the multi-engine design §4.6.3: any engine adapter
  * (Trino, Spark, DuckDB, ...) can compose this resolver with
  * its own `Engine` impl. The resolver doesn't care which
  * engine consumes the schema data — it just produces
  * portable `ResolvedSource` results. The integration test
  * wires `TrinoEngine + UnityCatalogSourceResolver` as the
  * reference composition; the same resolver works with a
  * future SparkEngine / DuckDBEngine / etc.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports (this is the Trino adapter — no Spark
  * dependencies). Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-trino/`
  */
class UnityCatalogSourceResolver(
    client:   UnityCatalogClient,
    identity: EngineIdentity,
) extends SourceResolver {

  override def resolve(source: SourceRef, callingIdentity: EngineIdentity): ResolvedSource = source match {

    // -- ByName: the portable case --
    case SourceRef.ByName(catalog, namespace, table) =>
      val cat = catalog.getOrElse(identity.name)  // default to engine's native catalog
      val sch = namespace.getOrElse("public")     // default to public schema
      val tbl = table

      client.describeTable(cat, sch, tbl) match {
        case Some(tableSchema) =>
          // Convert the UC columns to a portable ResolvedSchema
          // (Map[String, String]) per the existing ResolvedSource.Scan
          // shape. Stats are best-effort (None if unavailable).
          ResolvedSource.Scan(
            source = source,
            schema = ResolvedSchema(
              fields = tableSchema.columns.map(f => f.name -> ucTypeToString(f.dataType)).toMap,
            ),
          )

        case None =>
          ResolvedSource.NotFound(
            source = source,
            reason = s"Unity Catalog table '$cat.$sch.$tbl' not found or not accessible",
          )
      }

    // -- ByPath: not supported in Unity Catalog --
    case _: SourceRef.ByPath =>
      ResolvedSource.Incompatible(
        source = source,
        reason = "Unity Catalog does not support portable path-based sources; only registered tables (SourceRef.ByName) are supported",
      )

    // -- ByProvider: Spark-specific --
    case _: SourceRef.ByProvider =>
      ResolvedSource.Incompatible(
        source = source,
        reason = "ProviderRef is Spark-specific; Unity Catalog only supports registered tables (SourceRef.ByName)",
      )
  }

  /** Map a UC type-name string to a portable string suitable for
    * the portable `ResolvedSchema.fields: Map[String, String]`.
    *
    * We pass through the UC name (e.g. `"LONG"`, `"STRING"`,
    * `"DECIMAL"`) directly. This matches the existing
    * `TrinoSourceResolver.typeName` approach: the engine's
    * native type names are forwarded to consumers (which can
    * map them to their own engine-specific types as needed).
    *
    * ==Why passthrough (not mapping to `SealedDataType`)==
    *
    * Per scala-data-driven-refactor §1: the RESOLVED SCHEMA
    * carries engine-native type names. The mapping to
    * `SealedDataType` happens at COMPILE time, not at
    * RESOLVE time — `Engine.compile` knows which engine it's
    * compiling for and maps accordingly. The resolver stays
    * engine-portable by keeping the native name. */
  private def ucTypeToString(typeName: String): String = typeName
}

object UnityCatalogSourceResolver {

  /** Smart constructor: build a resolver with the given client
    * and engine identity. The identity is captured at
    * construction time so the resolver can use it for default
    * catalog / namespace resolution. */
  def apply(client: UnityCatalogClient, identity: EngineIdentity): UnityCatalogSourceResolver =
    new UnityCatalogSourceResolver(client, identity)
}