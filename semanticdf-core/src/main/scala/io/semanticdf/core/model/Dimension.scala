package io.semanticdf.core.model

import io.semanticdf.core.expr.Expr
import io.semanticdf.core.schema.SealedDataType

/** Engine-portable dimension ADT — Phase 2 contract. Mirrors the
  * design doc §4.4.1 "Dimension" (concrete case class per the
  * v0.3.0 design finding that pinned `Dimension.field(name, dataType)` as
  * the canonical smart constructor).
  *
  * A [[Dimension]] is a column-level grouping key — the user
  * groups by a dimension's value. The dimension carries the name,
  * the engine-portable expression that produces the value (the
  * `Expr`), and the declared type (for schema validation).
  *
  * ==Why a separate type from the existing `io.semanticdf.Dimension`==
  *
  * The spark-coupled `Dimension` carries a `SemanticScope => Column`
  * closure (Spark `Column` is engine-specific). The portable
  * `Dimension` carries an `Expr: Expr` (engine-portable). The two
  * coexist intentionally: the spark-coupled version is used by the
  * existing `SemanticTable.withDimensions(...)` API; the portable
  * version is used by the future `Model.of` API and the v2 manifest.
  *
  * Per karpathy §3 (surgical, no opportunistic refactors): the
  * existing `io.semanticdf.Dimension` is untouched.
  *
  * ==Why a smart constructor `Dimension.field(name, dataType)`==
  *
  * Per scala-data-driven-refactor §2 ("shape/validity separate"):
  * the smart constructor builds the common case
  * (`Expr.FieldRef(name)` + `Some(dataType)`) so callers don't have
  * to repeat the boilerplate. The unapply-style `Dimension(name, expr,
  * dataType)` is the structural constructor (for the less-common
  * case where the expression isn't a simple field ref, e.g. a
  * derived dimension `region` = `country.substr(0, 2)`).
  *
  * ==Why core (engine-portable)==
  *
  * Dimensions are universal across query engines. The engine-specific
  * compile (Spark's `Column = expr.fold(...)`, Trino's column
  * reference, etc.) lives in the engine adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: case class (no behavior)
  * - Equality auto-derived
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/Dimension.scala`
  */
final case class Dimension(
    name:     String,
    expr:     Expr,
    dataType: Option[SealedDataType] = None,
) extends Product with Serializable

object Dimension {

  /** Construct a simple field-reference dimension (the common case).
    *
    * `Dimension.field("region", SealedDataType.Varchar)` is equivalent
    * to `Dimension(name = "region", expr = Expr.FieldRef("region"),
    * dataType = Some(SealedDataType.Varchar))`.
    *
    * Used when the user wants to declare a dimension that maps
    * 1:1 to a source column. For derived dimensions (where the
    * expression isn't a simple field ref), use the structural
    * constructor `Dimension(name, expr, dataType)`. */
  def field(name: String, dataType: SealedDataType): Dimension =
    Dimension(name, Expr.FieldRef(name), Some(dataType))
}