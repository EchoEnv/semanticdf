package io.semanticdf

import org.apache.spark.sql.Column

/** Typed arithmetic for measure lambdas (Phase E3, see `docs/phase-E-plan.md`).
  *
  * The untyped form keeps working unchanged:
  * {{{
  *   Measure("avg_passengers", t => t("total_passengers") / t("flight_count"))
  * }}}
  *
  * The typed form is opt-in: wrap the operation in `divide` / `plus` / `minus` /
  * `multiply` and declare the static types of the operands (T, U) and the result
  * (R). The compiler resolves the implicit `Numeric[T]` / `Numeric[U]` /
  * `Numeric[R]` instances to type-check the operation. If any of T, U, R is
  * not a numeric type (e.g. `String`), the call fails to compile — catching
  * `t[String]("a") / t[String]("b")` and similar mistakes at build time.
  *
  * {{{
  *   import io.semanticdf.TypedArithmetic.divide
  *   Measure("avg_passengers", t =>
  *     divide[Long, Long, Double](t("total_passengers"), t("flight_count"))
  *   )
  * }}}
  *
  * '''Runtime cost: zero.''' The function body is the corresponding Spark
  * `Column` op (`/`, `+`, `-`, `*`). The type parameters are erased; the
  * `Numeric` instances are objects resolved at compile time. There is no
  * reflection, no `ClassTag`, no per-call state, no per-call allocation.
  * The compiled bytecode for the typed form is identical to the untyped
  * `t("a") / t("b")` form modulo the implicit-lookup cost (one-shot per
  * call site, not per row).
  *
  * '''No memory leak.''' The functions are pure Column ops. They don't
  * capture state, don't open resources, don't register listeners. Each
  * call returns a `Column` reference; nothing is retained past the
  * enclosing `Measure.expr` lambda's evaluation. */
object TypedArithmetic {

  /** Typed division. Compiles only when `Numeric[T]`, `Numeric[U]`, and
    * `Numeric[R]` are all in implicit scope (i.e. T, U, R are numeric types).
    * `String`, `Boolean`, and other non-numeric types fail at compile time. */
  def divide[T, U, R](a: Column, b: Column)(
      implicit nt: Numeric[T], nu: Numeric[U], nr: Numeric[R]
  ): Column = a / b

  /** Typed addition. See [[divide]] for the compile-time semantics. */
  def plus[T, U, R](a: Column, b: Column)(
      implicit nt: Numeric[T], nu: Numeric[U], nr: Numeric[R]
  ): Column = a + b

  /** Typed subtraction. */
  def minus[T, U, R](a: Column, b: Column)(
      implicit nt: Numeric[T], nu: Numeric[U], nr: Numeric[R]
  ): Column = a - b

  /** Typed multiplication. */
  def multiply[T, U, R](a: Column, b: Column)(
      implicit nt: Numeric[T], nu: Numeric[U], nr: Numeric[R]
  ): Column = a * b
}
