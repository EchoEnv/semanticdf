package io.semanticdf.mcp.handlers

import io.semanticdf.mcp.Envelope

/** `list_models` handler — the bootstrap tool. Reports which models the
  * server has loaded, with their human-readable descriptions.
  *
  * Per `mcp-contract.md` v2 §"Tool 1: list_models":
  *
  *   Request:  `{}`
  *   Response: `{"models": [{"name": "...", "description": "..."}, ...]}`
  *
  * Models are returned sorted alphabetically by name so the agent gets a
  * deterministic order (and so the response can be diffed across server
  * restarts without false positives from `Map` ordering).
  *
  * The handler takes no arguments — `request` is a `Unit`-shaped call. The
  * `name` parameter on the carrier resolves to the top-level model name
  * (not the YAML file's key) when set; falls back to the file key for
  * anonymous models.
  */
final class ListModels {

  /** Type the handler returns — mirrors the contract's response shape. */
  final case class Data(
      models: List[ModelSummary],
  )

  final case class ModelSummary(
      name: String,
      description: String,
  )

  def handle(registry: io.semanticdf.mcp.Models): Envelope[Data] = {
    val data = Data(
      models = registry.all.map { case (modelName, t) =>
        // The YamlLoader map key is the YAML top-level key — that's the
        // canonical model name. Use it directly; fall back to the
        // SemanticTable's own name field only if the loader key is empty
        // (shouldn't happen, but defensive).
        val name = if (modelName.nonEmpty) modelName
                   else t.name.getOrElse("<unnamed>")
        ModelSummary(
          name = name,
          description = t.description.getOrElse(""),
        )
      },
    )
    Envelope.ok(data)
  }
}
