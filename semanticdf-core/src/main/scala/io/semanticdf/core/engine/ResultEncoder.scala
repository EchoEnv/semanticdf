package io.semanticdf.core.engine

/** Engine-portable result-encoder trait \u2014 Phase 2 contract.
  * Mirrors the design doc \u00a74.5.4 "ResultEncoder[-R]" (the
  * boundary that translates engine-native results into the
  * portable `PortableQueryResult`).
  *
  * ==Why a trait (not a method on `Engine`)==
  *
  * Per scala-data-driven-refactor \u00a71: data in core, behavior
  * in adapters. The `PortableQueryResult` shape is the data;
  * the encoding (Trino ResultSet \u2192 `PortableQueryResult` vs
  * DuckDB ResultSet \u2192 `PortableQueryResult`) is the behavior.
  * The trait lives in core (the SHAPE), the implementations
  * live in each engine adapter (the BODY).
  *
  * ==Why `[-R]` (contravariant)==
  *
  * `ResultEncoder[TrinoResult]` and `ResultEncoder[DuckDBResult]`
  * are both `ResultEncoder[Any]` (any specific R is a subtype of
  * Any). A function that consumes `Any` can be used in any
  * context expecting a more specific R. This is the standard
  * "consumer is contravariant in its input" Scala convention.
  *
  * ==Why `Either[ResultError, PortableQueryResult]` (not `Try`)==
  *
  * Per scala-data-driven-refacer \u00a72 ("shape and validity
  * separate"): the encoding either succeeds (returns the
  * portable result) or fails with a typed error. `Try` would
  * carry an `Exception`; the typed `ResultError` ADT lets the
  * consumer match on the failure case distinctly from the
  * runtime-exception case. */
trait ResultEncoder[-R] {

  /** Encode an engine-native result into a
    * `PortableQueryResult`. Returns `Left[ResultError]` on
    * any encoding failure (the design \u00a74.5.4 conformance
    * property: a failed encoding is a typed error, not a
    * thrown exception). */
  def encode(r: R): Either[ResultError, PortableQueryResult]
}

/** Engine-portable result-error ADT \u2014 the failure shape of
  * `ResultEncoder.encode`. Mirrors the design doc \u00a74.5.4. */
sealed trait ResultError extends Product with Serializable

object ResultError {

  /** A required column is missing from the result. */
  final case class ColumnMissing(name: String) extends ResultError

  /** A column's value could not be decoded (e.g. unexpected
    * JDBC type, out-of-range value). */
  final case class ValueDecodeFailed(column: String, reason: String) extends ResultError

  /** The native result has the wrong shape (e.g. the engine
    * returned more columns than the schema declared). */
  final case class ShapeMismatch(reason: String) extends ResultError
}