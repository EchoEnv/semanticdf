package io.semanticdf.trino

import io.semanticdf.core.engine.{EngineContext, EngineError, EngineIdentity, MCPEngineProvider, MCPQueryRequest, PortableQueryResult}
import io.semanticdf.core.model.Model
import io.semanticdf.core.rel.SortKey


import java.util.{List => JList}
import scala.jdk.CollectionConverters._

/** `MCPEngineProvider` impl backed by the Trino engine.
  *
  * Holds a `TrinoEngine` + a name \u2192 `Model` registry. On `query`:
  *   1. Look up the `Model` by name
  *   2. Build a `MCPQueryRequest` \u2192 `TrinoQueryRequest` adapter
  *   3. Run through the Trino engine's `compile` + `execute` path
  *   4. Encode to `PortableQueryResult`
  *
  * ==Why this lives in the trino adapter (not core)==
  *
  * Per scala-data-driven-refactor \u00a71: behavior in adapters. The
  * Trino-specific path (compile SQL, run JDBC, decode `TrinoResult`)
  * is engine-specific. The TRAIT (`MCPEngineProvider`) lives in
  * core; the IMPL lives here.
  *
  * ==Why `available` is `engine != null && connectionFactory.isDefined`==
  *
  * Per the design: a provider is unavailable if its `Engine`
  * wasn't constructed (the engine field is null) OR if the
  * connection factory isn't configured. Both are startup-time
  * misconfigurations that should fail loud at boot (per the
  * registry's `require` at construction). */
final class TrinoEngineProvider(
    private val engine: TrinoEngine,
    private val modelRegistry: Map[String, Model],
) extends MCPEngineProvider {

  override val identity: EngineIdentity = EngineIdentity(
    name                 = "trino",
    nativeVersion        = "0.286",
    engineAdapterVersion = "0.2.4",
  )

  override val available: Boolean =
    engine != null && engine.connectionFactory.isDefined

  override def query(
      model:   Model,
      request: MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, PortableQueryResult] = {
    modelRegistry.get(request.model) match {
      case None => Left(EngineError.FeatureDeferred(
        feature = s"trino.provider.model-not-found:${request.model}",
        release = "v0.5.0",
      ))
      case Some(m) => runQuery(m, request, ctx)
    }
  }

  override def explain(
      model:   Model,
      request: MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, String] = {
    modelRegistry.get(request.model) match {
      case None => Left(EngineError.FeatureDeferred(
        feature = s"trino.provider.model-not-found:${request.model}",
        release = "v0.5.0",
      ))
      case Some(m) => engine.explain(m, ctx).map(_.split('\n').mkString(" "))
    }
  }

  /** Run the Trino query and encode the result to
    * `PortableQueryResult`. For v1 we route through the
    * existing `TrinoEngine.compile` + `execute` path (which
    * already produces `TrinoResult`), then encode via
    * `TrinoResultEncoder` (added in PR #400). */
  private def runQuery(
      model:   Model,
      request: MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, PortableQueryResult] = {
    engine.compile(model, ctx).flatMap { plan =>
      engine.execute(plan, ctx).map { raw =>
        new TrinoResultEncoder().encode(raw.asInstanceOf[TrinoResult]) match {
          case Right(pqr) => pqr
          case Left(err) =>
            // Fall back to empty result with the encoding error in
            // metadata. Per scala-data-driven-refacer \u00a71: errors are
            // data, not exceptions. A future PR can surface this as
            // a proper EngineError.
            PortableQueryResult.empty.copy(metadata = Map("encoding.error" -> err.toString))
        }
      }
    }
  }
}