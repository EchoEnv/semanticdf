package io.semanticdf.adapters

/** The intermediate produced by [[SDFAdapter.parse]].
  *
  * Holds the raw manifest JSON plus the metadata extracted during
  * `parse` (kind, source table names) so `toSemanticTables` can route
  * to the right reader without re-parsing.
  *
  * Two forms are supported:
  *   - `single` — a single-table manifest; `source` is the table to load
  *   - `joined` — a joined manifest; `leftSource` and `rightSource`
  *     are the per-side tables to load
  *
  * The JSON text is preserved so the existing
  * [[io.semanticdf.SemanticManifest.fromJso[[SDFAdapter.pars[[SDFAdapter.parse]]]] and
  * [[io.semanticdf.SemanticManifest.fromJoinedJso[[SDFAdapter.pars[[SDFAdapter.parse]]]] methods can
  * do the actual reconstruction — the adapter is a delegation layer,
  * not a re-implementation. */
final case class SDFProject(
    /** The raw manifest JSON. Held as a string (not a parsed tree)
      * so the existing `fromJson` / `fromJoinedJson` methods can
      * consume it directly. */
    text:        String,
    /** `"single"` or `"joined"`. Detected from the `kind` field at parse time. */
    kind:        String,
    /** Single-table form: the source table to load. */
    source:      Option[String] = None,
    /** Joined form: the left-side source table to load. */
    leftSource:  Option[String] = None,
    /** Joined form: the right-side source table to load. */
    rightSource: Option[String] = None,
)
