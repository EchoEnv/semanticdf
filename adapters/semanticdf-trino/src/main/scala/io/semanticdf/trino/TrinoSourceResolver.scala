package io.semanticdf.trino

import io.semanticdf.core.engine.{EngineIdentity, ResolvedSchema, ResolvedSource, SourceResolver, SourceStats}
import io.semanticdf.core.model.SourceRef
import io.semanticdf.core.schema.SealedDataType

/** Engine-specific Trino source-resolver implementation.
  *
  * Implements the engine-portable `SourceResolver` contract
  * (from `io.semanticdf.core.engine`) using a Trino-specific
  * `TrinoClient`. This is the FIRST real engine adapter
  * implementation — concrete behavior, not a placeholder.
  *
  * ==What this class does==
  *
  * Resolves portable `SourceRef`s to engine-specific
  * `ResolvedSource` results by calling Trino:
  *   - `SourceRef.ByName` → `DESCRIBE <catalog>.<schema>.<table>`
  *     (via `TrinoClient.describeTable`)
  *   - `SourceRef.ByPath` → REJECTED (Trino doesn't have
  *     portable path-based sources — this resolver only
  *     handles ByName)
  *   - `SourceRef.ByProvider` → REJECTED (the `ProviderRef`
  *     mechanism is Spark-specific; Trino doesn't support
  *     portable DataFrame providers)
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
  * `describeTable` API — both return `None`. A future PR with
  * a richer TrinoClient (one that surfaces auth errors
  * distinctly) would map `None` to `AuthFailed` when the
  * reason is auth-related. For now, `None` maps to `NotFound`.
  *
  * ==Why an injected `TrinoClient`==
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior
  * lives elsewhere"): the BEHAVIOR (calling Trino) lives here.
  * The `TrinoClient` trait (in the same package) is the
  * boundary that makes this class unit-testable without a real
  * Trino cluster. Tests inject a `FakeTrinoClient`.
  *
  * ==Why `extends SourceResolver`==
  *
  * The portable contract is in core (`SourceResolver`). This
  * class implements that contract. The `Engine.compile` method
  * can resolve a `Model.source` via this resolver before
  * emitting SQL — the resolver's result feeds into the plan.
  *
  * ==Why the `FromPath` / `FromProvider` cases return `Incompatible`==
  *
  * Per the design's `ProviderRef.DataFrameSource`:
  * "DataFrame-specific provider refs return IncompatibleEngine
  * on non-Spark engines." Similarly, `ByPath` is Spark-centric
  * (Trino doesn't have a portable path-based source). Both
  * cases are explicitly rejected here.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports (this is the Trino adapter — no Spark
  * dependencies). Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-trino/`
  */
class TrinoSourceResolver(
    client:   TrinoClient,
    identity: EngineIdentity,
) extends SourceResolver {

  override def resolve(source: SourceRef, callingIdentity: EngineIdentity): ResolvedSource = source match {

    // -- ByName: the portable case --
    case SourceRef.ByName(catalog, namespace, table) =>
      val cat  = catalog.getOrElse(identity.name)  // default to engine's native catalog
      val sch  = namespace.getOrElse("public")     // default to public schema
      val tbl  = table

      client.describeTable(cat, sch, tbl) match {
        case Some(tableSchema) =>
          // Convert the Trino columns to a portable ResolvedSchema
          // (Map[String, String]) per the existing ResolvedSource.Scan
          // shape. Stats are best-effort (None if unavailable).
          val stats = client.getTableRowCount(cat, sch, tbl).map { rows =>
            SourceStats(estimatedRows = Some(rows))
          }
          ResolvedSource.Scan(
            source = source,
            schema = ResolvedSchema(
              fields = tableSchema.columns.map(f => f.name -> typeName(f.dataType)).toMap,
            ),
          )

        case None =>
          ResolvedSource.NotFound(
            source = source,
            reason = s"Trino table '$cat.$sch.$tbl' not found or not accessible",
          )
      }

    // -- ByPath: not supported on Trino --
    case SourceRef.ByPath(format, path, options) =>
      ResolvedSource.Incompatible(
        source = source,
        reason = s"Trino engine does not support path-based sources (format='$format', path='$path'). Use ByName with a Trino catalog instead.",
      )

    // -- ByProvider: not supported on Trino (ProviderRef is Spark-specific) --
    case SourceRef.ByProvider(provider) =>
      ResolvedSource.Incompatible(
        source = source,
        reason = s"Trino engine does not support portable ProviderRefs (provider='$provider'). Use ByName with a Trino catalog instead.",
      )
  }

  /** Map a portable `SealedDataType` to its Trino type name
    * (for the `ResolvedSchema` Map[String, String] field).
    * The `ResolvedScan.fields` (the richer Seq[Field] shape from
    * PR #358) carries the full Field; the `ResolvedSchema.fields`
    * (the simpler Map from PR #357) carries just type names —
    * used for diagnostics).
    *
    * This mapping is the inverse of what the `SqlLowerer` does
    * for predicates: SQL types in → portable types out. The
    * portable types here are mapped BACK to SQL type names for
    * the `ResolvedSchema` field.
    */
  private def typeName(t: SealedDataType): String = t match {
    case SealedDataType.BigInt        => "bigint"
    case SealedDataType.Int           => "integer"
    case SealedDataType.Double        => "double"
    case SealedDataType.Varchar       => "varchar"
    case SealedDataType.Boolean       => "boolean"
    case SealedDataType.Date          => "date"
    case SealedDataType.Timestamp     => "timestamp"
    case SealedDataType.Decimal(_, _) => "decimal"
    case SealedDataType.Array(_)      => "array"
    case SealedDataType.Map(_, _)     => "map"
    case SealedDataType.Row(_)        => "row"
    case SealedDataType.Json          => "json"
  }
}

object TrinoSourceResolver {

  /** Smart constructor: build a resolver with the given client
    * and engine identity. The identity is captured at
    * construction time so the resolver can use it for default
    * catalog / namespace resolution. */
  def apply(client: TrinoClient, identity: EngineIdentity): TrinoSourceResolver =
    new TrinoSourceResolver(client, identity)
}