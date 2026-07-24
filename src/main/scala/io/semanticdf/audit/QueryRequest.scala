package io.semanticdf.audit

import io.semanticdf.Predicate

/** Audit-side capture of a user's query request.
  *
  * Populated by [[io.semanticdf.SemanticTable.query]] (and the streaming
  * variants) and preserved across the fluent chain so the audit event
  * carries the user's original intent — not the post-chain op tree.
  *
  * Field set is intentionally minimal: only what the audit event needs.
  * Order, limit, time grain, and time range are not in the audit event
  * (they don't help answer "is this query the same as the last one?").
  * If the user adds more fields here, the audit event should use them;
  * if not, leave them out.
  *
  * Lives in the `audit` package so the library core stays small.
  * The library depends on `audit`; `audit` does not depend on the
  * library's internals. */
final case class QueryRequest(
    model:      String,
    measures:   Seq[String] = Seq.empty,
    dimensions: Seq[String] = Seq.empty,
    where:      Option[Predicate] = None,
    having:     Option[Predicate] = None,
)
