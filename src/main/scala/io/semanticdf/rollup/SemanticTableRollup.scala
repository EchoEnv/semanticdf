package io.semanticdf.rollup

import io.semanticdf.SemanticTable

/** Methods on [[SemanticTable]] for the rollups feature (v0.2.4 redesign).
  *
  * In the previous design (PR #328 / #329), `useRollup` returned a
  * `SemanticTable` with `root = SemanticRollupOp(...)`, which intertwined
  * rollup state with the existing op tree and caused 5 audit cycles
  * with 19+ HIGH-severity bugs. The new design returns a separate
  * [[RollupQuery]] type that has NO interaction with the existing fluent
  * chain.
  *
  * Split into a separate trait (mirroring `SemanticTableCollection`,
  * `SemanticTableStreaming`, etc.) to keep `SemanticTable.scala` itself
  * lean.
  */
private[semanticdf] trait SemanticTableRollup { self: SemanticTable =>

  /** Register a pre-aggregated rollup table. Adds to the model's
    * `rollups` list. The [[Rollup]] value class is pure data; the
    * actual DataFrame source is registered separately with a
    * [[RollupRegistry]] at query time.
    *
    * If a rollup with the same name already exists, this REPLACES it
    * (the new rollup's stats precompute overwrites the old). The user
    * should re-register after rebuilding the underlying rollup table.
    *
    * Throws `IllegalArgumentException` if the rollup's `baseModel`
    * doesn't match this model's name.
    *
    * == Joins drop rollups (Path-2 contract) ==
    * Rollups do NOT survive joins. After `join_one` / `join_many` /
    * `join_cross`, the joined model has empty rollups.
    *
    * If you need a rollup that targets the joined model: build a
    * separate `Rollup` with `baseModel = joined.name` and re-register it
    * via `joined.withRollup(...)`. Note that anonymous joined models
    * (those not constructed via `toSemanticTable(..., name = Some(...))`)
    * cannot host rollups because `withRollup` requires a named model.
    *
    * Rationale (per the v0.2.4 redesign): `RollupQuery.execute` reads
    * from the rollup source's pre-aggregated DataFrame, not the joined
    * op tree — so preserving a rollup across the join would silently
    * drop the join. Making this loud (joined model has empty rollups;
    * `useRollup` on it throws) preserves the "fail fast" contract.
    */



  // ---- Implementation (mixed into SemanticTable) ----

  def withRollup(rollup: Rollup): SemanticTable = {
    require(name.contains(rollup.baseModel),
      s"Rollup '${rollup.name}' baseModel '${rollup.baseModel}' " +
      s"does not match model name '${name.getOrElse("anonymous")}' " +
      s"(rollups can only be registered on named models)")
    val newRollups = rollups.filterNot(_.name == rollup.name) :+ rollup
    new SemanticTable(root, postAggPredicates, version, sourceTable, status,
      auditSink, auditRequest, resultCache, maxRows, broadcastJoinThreshold,
      salt, materializeLevel, rollups = newRollups)
  }

  def findRollup(name: String): Option[Rollup] = rollups.find(_.name == name)

  def listRollups(): List[Rollup] = rollups

  def useRollup(name: String, registry: RollupRegistry): RollupQuery = {
    val rollup = findRollup(name).getOrElse(throw new IllegalArgumentException(
      s"No rollup named '$name' registered on model " +
      // `name` here refers to the SemanticTable field, NOT the parameter
      // (parameters shadow fields in Scala 2). Disambiguate via `this.name`.
      s"'${this.name.getOrElse("<anonymous>")}'. " +
      s"Available: ${rollups.map(_.name).mkString("[", ", ", "]")}. " +
      // Help diagnose the most common cause: joined models have empty
      // rollups (Path-2 contract). `withRollup` happens to silently
      // no-op on path-a (rollups already on `this`), but is loud on
      // path-b (joined has empty rollups → useRollup throws here).
      s"Common cause: this model was created via join_one / join_many / " +
      s"join_cross — joined models have empty rollups by design (see " +
      s"`joinRollups` Scaladoc). To target a rollup at the joined shape, " +
      s"construct a separate `Rollup(baseModel = joined.name, ...)` " +
      s"and call `joined.withRollup(...)`."
    ))
    require(registry.contains(name),
      s"Rollup '$name' not registered in the supplied RollupRegistry. " +
      s"Use `RollupRegistry.register(name, provider)` before `useRollup`.")
    new RollupQuery(this, rollup, registry)
  }
}
