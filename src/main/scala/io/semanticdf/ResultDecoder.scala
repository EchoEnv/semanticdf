package io.semanticdf

import org.apache.spark.sql.Row

/** Typeclass for decoding a single Spark `Row` into a typed value `T`.
  *
  * The standard way to consume a query result is `t.execute(spark).collect()`
  * — but `Row` is untyped (everything comes back as `Any`). `ResultDecoder[T]`
  * lets callers say "I want `Seq[Foo]`" and get a compile-time-checked
  * decoder that knows how to project a `Row` into `Foo`.
  *
  * For multi-column results (tuples, case classes), the caller supplies a
  * decoder that reads the right column indices. For single-column results,
  * the built-in primitive instances (`ResultDecoder.stringDecoder`, etc.)
  * read column 0 and are the most common case.
  *
  * Example:
  * {{{
  * // Built-in: reads column 0 as a String.
  * val names: Seq[String] = table.execute(spark).collectAs[String]
  *
  * // Custom: reads columns 0 and 1 as a tuple.
  * implicit val pairDecoder: ResultDecoder[(String, Long)] = new ResultDecoder[(String, Long)] {
  *   def decode(row: Row): (String, Long) = (row.getString(0), row.getLong(1))
  * }
  * val pairs: Seq[(String, Long)] = table.execute(spark).collectAs[(String, Long)]
  * }}}
  *
  * Case classes are out of scope for the built-in instances (Scala 2.13
  * has no native deriving — would need shapeless or runtime reflection).
  * For case-class results, use Spark's built-in `df.as[MyCaseClass]` with
  * an `org.apache.spark.sql.Encoder` — it's the right tool there.
  *
  * Why a typeclass and not a method on `DataFrame`? Because the decoder
  * is a *policy* the caller chooses, not a property of the DataFrame.
  * Multiple decoders can coexist for the same shape (e.g. one that reads
  * `getString(0)` and one that reads `getAs[String](0)` for null safety).
  * Implicit scope keeps the call site clean: `collectAs[T]` just works
  * for any `T` with an `implicit ResultDecoder[T]` in scope. */
trait ResultDecoder[T] {
  /** Decode one row into a typed value. Must not throw on missing columns —
    * callers should pre-validate the query plan matches the decoder's shape. */
  def decode(row: Row): T
}

object ResultDecoder {
  /** Summoner — bring an implicit ResultDecoder into scope. */
  def apply[T](implicit ev: ResultDecoder[T]): ResultDecoder[T] = ev

  // ---------------------------------------------------------------------------
  // Built-in primitive instances — all read column 0.
  //
  // These cover the most common case: a single-column query (e.g.
  // `t.groupBy("carrier").aggregate("flight_count")` where you only want
  // the measure column). For multi-column queries, write a custom decoder.
  // ---------------------------------------------------------------------------

  implicit val stringDecoder: ResultDecoder[String]   = _.getString(0)
  implicit val intDecoder: ResultDecoder[Int]         = _.getInt(0)
  implicit val longDecoder: ResultDecoder[Long]       = _.getLong(0)
  implicit val doubleDecoder: ResultDecoder[Double]   = _.getDouble(0)
  implicit val floatDecoder: ResultDecoder[Float]     = _.getFloat(0)
  implicit val booleanDecoder: ResultDecoder[Boolean] = _.getBoolean(0)
}