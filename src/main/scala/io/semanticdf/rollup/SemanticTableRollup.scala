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
    * `join_cross`, the joined model has empty rollups — re-register
    * via `joined.withRollup(...)` if you need a rollup that targets
    * the joined model. Rationale (per the v0.2.4 redesign):
    * `RollupQuery.execute` reads from the rollup source's pre-aggregated
    * DataFrame, not the joined op tree — so preserving a rollup across
    * the join would silently drop the join. Making this loud (joined
    * model has empty rollups; `useRollup` on it throws) preserves the
    * "fail fast" contract.
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
      s"No rollup named '$name' registered on this model. " +
      s"Available: ${rollups.map(_.name).mkString(", ")}"))
    require(registry.contains(name),
      s"Rollup '$name' not registered in the supplied RollupRegistry. " +
      s"Use `RollupRegistry.register(name, provider)` before `useRollup`.")
    new RollupQuery(this, rollup, registry)
  }
}
