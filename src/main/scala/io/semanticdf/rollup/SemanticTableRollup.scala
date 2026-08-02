package io.semanticdf.rollup

import io.semanticdf.{SemanticRollupOp, SemanticTable}

/** Methods on [[SemanticTable]] for managing pre-aggregated rollups.
  *
  * Split into a separate trait (mirroring `SemanticTableCollection`,
  * `SemanticTableStreaming`, etc.) to keep `SemanticTable.scala` itself
  * lean. v0.2.4 ships manual rollups only -- the user explicitly
  * names the rollup via [[SemanticTable.useRollup]]. Auto-routing
  * (`findRollupMatch`) is v0.3.x+.
  *
  * Design: `useRollup(name, registry)` returns a NEW SemanticTable with
  * `root = SemanticRollupOp(rollup, registry)`. The `SemanticRollupOp`
  * is a terminal op (it IS the root, not a wrapping op), so downstream
  * shape-changers like `where()` and `orderBy()` either:
  *   (a) refuse to apply (throw) because there's nothing to transform, OR
  *   (b) wrap the root and re-apply after rollup projection
  *
  * For v0.2.4 simplicity, we go with (b): `query()` and the shape-changers
  * wrap the existing root. Since the rollup is already pre-aggregated, the
  * "aggregation" in the wrapped op is a no-op pass-through. This preserves
  * the fluent chain end-to-end.
  *
  * The `registry` travels with the returned SemanticTable. It is NOT
  * Serializable (DataFrame providers are not Serializable). This is a
  * documented limitation, same as `AuditSink`.
  */
private[semanticdf] trait SemanticTableRollup { self: SemanticTable =>

  /** Register a pre-aggregated rollup table. Adds to the model's
    * `rollups` list. The `Rollup` value class is pure data (Serializable);
    * the actual DataFrame source is registered separately with a
    * [[RollupRegistry]] at query time.
    *
    * If a rollup with the same name already exists, this REPLACES it
    * (the new rollup's stats precompute overwrites the old). The user
    * should re-register after rebuilding the underlying rollup table.
    *
    * Throws `IllegalArgumentException` if the rollup's `baseModel`
    * doesn't match this model's name.
    */
  def withRollup(rollup: Rollup): SemanticTable = {
    require(rollup.baseModel == name.getOrElse(rollup.baseModel),
      s"Rollup '${rollup.name}' baseModel '${rollup.baseModel}' " +
      s"does not match model name '${name.getOrElse("")}'")
    val newRollups = rollups.filterNot(_.name == rollup.name) :+ rollup
    new SemanticTable(root, postAggPredicates, version, sourceTable, status,
      auditSink, auditRequest, resultCache, maxRows, broadcastJoinThreshold,
      salt, materializeLevel, rollups = newRollups)
  }

  /** List all rollups registered on this model (declaration order,
    * newest last). Pure-data -- no DataFrame references. */
  def listRollups(): List[Rollup] = rollups

  /** Look up a rollup by name. */
  def findRollup(name: String): Option[Rollup] = rollups.find(_.name == name)

  /** Mark this table as "use rollup `name` when executed".
    *
    * Returns a new SemanticTable with `root = SemanticRollupOp(rollup, registry)`.
    * The `SemanticRollupOp` is a terminal op -- it holds the registry
    * (NOT serializable, documented limitation).
    *
    * Throws `IllegalArgumentException` if `name` doesn't match any
    * registered rollup, or if the registry doesn't contain `name`.
    */
  def useRollup(name: String, registry: RollupRegistry): SemanticTable = {
    val rollup = findRollup(name).getOrElse(throw new IllegalArgumentException(
      s"No rollup named '$name' registered on this model. " +
      s"Available: ${rollups.map(_.name).mkString(", ")}"))
    require(registry.contains(name),
      s"Rollup '$name' not registered in the supplied RollupRegistry. " +
      s"Use `RollupRegistry.register(name, provider)` before `useRollup`.")
    new SemanticTable(SemanticRollupOp(rollup, registry), postAggPredicates,
      version, sourceTable, status, auditSink, auditRequest, resultCache,
      maxRows, broadcastJoinThreshold, salt, materializeLevel, rollups = rollups)
  }
}