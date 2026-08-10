package io.semanticdf.duckdb

import io.semanticdf.core.engine.{EngineContext, EngineError, EngineIdentity, MCPEngineProvider, MCPQueryRequest, PortableQueryResult}
import io.semanticdf.core.model.Model

/** `MCPEngineProvider` impl backed by the DuckDB engine.
 *
 *  v0.3.1: the engine-portable path for DuckDB queries. Holds
 *  a `DuckDBEngine` + a name→`Model` registry.
 */
final class DuckDBEngineProvider(
    private val engine: DuckDBEngine,
    private val modelRegistry: Map[String, Model],
) extends MCPEngineProvider {

  override val identity: EngineIdentity = EngineIdentity(
    name                 = "duckdb",
    nativeVersion        = "0.10.0",
    engineAdapterVersion = "0.3.0",
  )

  override val available: Boolean =
    engine != null && engine.connectionFactory.isDefined

  override def query(
      model:   Model,
      request: MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, PortableQueryResult] = {
    modelRegistry.get(request.model) match {
      case None => Left(EngineError.ModelNotFound(request.model))
      case Some(m) => runQuery(m, ctx)
    }
  }

  override def explain(
      model:   Model,
      request: MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, String] = {
    modelRegistry.get(request.model) match {
      case None => Left(EngineError.ModelNotFound(request.model))
      case Some(m) => engine.explain(m, ctx).map(_.split('\n').mkString(" "))
    }
  }

  private def runQuery(
      model: Model,
      ctx:   EngineContext,
  ): Either[EngineError, PortableQueryResult] = {
    engine.compile(model, ctx).flatMap { plan =>
      engine.execute(plan, ctx).map { raw =>
        new DuckDBResultEncoder().encode(raw.asInstanceOf[DuckDBResult]) match {
          case Right(pqr) => pqr
          case Left(_) =>
            PortableQueryResult.empty.copy(metadata = Map("encoding.error" -> "true"))
        }
      }
    }
  }
}
