package io.semanticdf.core.field

/** Engine-portable sort-key ADT — Phase 1 consolidation mirror.
  *
  * Mirrors `io.semanticdf.SortKey` with ONLY the data parts. The
  * Spark-bearing `toColumn` behavior stays in the original; the
  * `core` version is just the sealed trait + case classes + name
  * accessor.
  *
  * ==Why this exists==
  *
  * Sort keys are an engine-portable concept — every database engine
  * has the notion of "ascending vs descending sort by a column".
  * The data shape (Asc(name) | Desc(name)) is the contract; the
  * engine-specific compile (Spark `col(name).asc()`,
  * Trino `ORDER BY name ASC`, etc.) lives in the engine adapter.
  *
  * Future engine adapters (Trino, Databricks) need to know the
  * data shape without depending on Spark. This core mirror provides
  * that shape.
  *
  * ==Boundary contract==
  *
  * This file compiles with zero `org.apache.spark.*` imports.
  * The case classes carry only a `String name` field — no engine
  * types, no closures, no compile methods.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 2 case classes (name + direction)
  * - Equality auto-derived (case classes)
  * - Hash codes stable (auto-derived)
  * - Parallel to `io.semanticdf.SortKey` (data only; the original
  *   adds `toColumn` for the Spark path)
  */
sealed trait SortKey {
  /** The column name this sort key refers to. */
  def name: String
}

object SortKey {

  /** Ascending sort key. */
  final case class Asc(name: String) extends SortKey

  /** Descending sort key. */
  final case class Desc(name: String) extends SortKey

  /** Read the column-name field of any SortKey. Available publicly on
    * the core mirror (it was `private[semanticdf]` in the original to
    * avoid exposing the sealed cases). On the core mirror we can
    * expose it freely because the case classes are themselves
    * public on the core mirror — the privacy was a Spark-side
    * concern to keep the sealed trait sealed from end users, which
    * doesn't apply to engine-portable consumers. */
  def nameOf(k: SortKey): String = k match {
    case Asc(n)  => n
    case Desc(n) => n
  }
}