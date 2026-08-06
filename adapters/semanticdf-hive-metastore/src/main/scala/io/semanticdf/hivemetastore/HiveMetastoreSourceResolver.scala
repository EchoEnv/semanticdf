package io.semanticdf.hivemetastore

import io.semanticdf.core.engine.{EngineIdentity, ResolvedSchema, ResolvedSource, SourceResolver}
import io.semanticdf.core.model.SourceRef

/** Engine-specific Hive Metastore source-resolver implementation.
  *
  * Implements the engine-portable `SourceResolver` contract
  * (from `io.semanticdf.core.engine`) using HMS's Thrift RPC.
  * Mirrors `UnityCatalogSourceResolver` (PR #394) almost
  * verbatim — same shape, different transport.
  *
  * ==What this class does==
  *
  * Resolves portable `SourceRef`s to engine-portable
  * `ResolvedSource` results by calling HMS:
  *   - `SourceRef.ByName` → `getTable(catalog, database, table)`
  *     (via `HiveMetastoreClient.describeTable`)
  *   - `SourceRef.ByPath` → REJECTED (HMS doesn't have portable
  *     path-based sources — only registered tables)
  *   - `SourceRef.ByProvider` → REJECTED (the `ProviderRef`
  *     mechanism is Spark-specific; HMS doesn't support
  *     portable DataFrame providers)
  *
  * ==Why an injected `HiveMetastoreClient`==
  *
  * Per scala-data-driven-refacer §1 ("data is data, behavior
  * lives elsewhere"): the BEHAVIOR (calling HMS, parsing
  * responses) lives here. The `HiveMetastoreClient` trait
  * (same package) is the boundary that makes this class
  * unit-testable without a real HMS. Tests inject a
  * `FakeHiveMetastoreClient`.
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
  * (HMS doesn't have a portable path-based source — only
  * registered tables). Both cases are explicitly rejected.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
class HiveMetastoreSourceResolver(
    client:   HiveMetastoreClient,
    identity: EngineIdentity,
) extends SourceResolver {

  override def resolve(source: SourceRef, callingIdentity: EngineIdentity): ResolvedSource = source match {

    // -- ByName: the portable case --
    case SourceRef.ByName(catalog, namespace, table) =>
      // HMS terminology: "catalog" (HMS 3.x) = "warehouse" in
      // some configs. For the common case (HMS 3.x default),
      // catalog defaults to engine's native catalog (often
      // "hive" for the default HMS instance).
      val cat = catalog.getOrElse(identity.name)
      // "database" = HMS's term for "schema".
      val db  = namespace.getOrElse("default")
      val tbl = table

      client.describeTable(cat, db, tbl) match {
        case Some(tableSchema) =>
          // Convert the HMS columns to a portable ResolvedSchema
          // (Map[String, String]) per the existing ResolvedSource.Scan
          // shape. HMS type names pass through (engine consumers
          // map them to their native types as needed).
          ResolvedSource.Scan(
            source = source,
            schema = ResolvedSchema(
              fields = tableSchema.columns.map(f => f.name -> f.dataType).toMap,
            ),
          )

        case None =>
          ResolvedSource.NotFound(
            source = source,
            reason = s"Hive Metastore table '$cat.$db.$tbl' not found or not accessible",
          )
      }

    // -- ByPath: not supported in HMS --
    case _: SourceRef.ByPath =>
      ResolvedSource.Incompatible(
        source = source,
        reason = "Hive Metastore does not support portable path-based sources; only registered tables (SourceRef.ByName) are supported",
      )

    // -- ByProvider: Spark-specific --
    case _: SourceRef.ByProvider =>
      ResolvedSource.Incompatible(
        source = source,
        reason = "ProviderRef is Spark-specific; Hive Metastore only supports registered tables (SourceRef.ByName)",
      )
  }
}

object HiveMetastoreSourceResolver {

  /** Smart constructor: build a resolver with the given client
    * and engine identity. The identity is captured at
    * construction time so the resolver can use it for default
    * catalog / database resolution. */
  def apply(client: HiveMetastoreClient, identity: EngineIdentity): HiveMetastoreSourceResolver =
    new HiveMetastoreSourceResolver(client, identity)
}