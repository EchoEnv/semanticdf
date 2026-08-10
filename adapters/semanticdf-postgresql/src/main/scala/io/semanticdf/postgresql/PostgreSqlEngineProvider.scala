package io.semanticdf.postgresql

import io.semanticdf.core.engine.{EngineContext, EngineError, EngineIdentity, MCPEngineProvider, MCPQueryRequest, PortableQueryResult}
import io.semanticdf.core.model.Model

/** `MCPEngineProvider` impl backed by the PostgreSQL engine.
 *
 *  v0.3.1: the engine-portable path for PostgreSQL queries.
 */
final class PostgreSqlEngineProvider(
    private val engine: PostgreSqlEngine,
    private val database: String,
    private val modelRegistry: Map[String, Model] = Map.empty,
) extends MCPEngineProvider {

  override val identity: EngineIdentity = EngineIdentity(
    name                 = s"postgresql:$database",
    nativeVersion        = "16.0",
    engineAdapterVersion = "0.3.0",
  )

  override val available: Boolean = engine != null

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
      engine.executePortable(plan, ctx)
    }
  }
}
