package io.semanticdf.core.field

/** Engine-portable mirror of `io.semanticdf.MeasureKind`.
  *
  * Phase 1 consolidation: pure-data sealed ADT that classifies a measure
  * within a semantic model. Same data shape as the Spark-bearing original;
  * no engine behavior, no compile methods.
  *
  * ==Why this exists==
  *
  * Future engine adapters (Trino, Databricks, custom-platform) need a way
  * to classify measures without depending on Spark. This mirror provides
  * that classification engine-portably.
  *
  * The original `io.semanticdf.MeasureKind` remains the canonical Spark-
  * facing type (it may grow engine-coupled methods in the future). Engine-
  * portable consumers (e.g. wire-format encoders, JSON serializers, the
  * MCP `describe_model.measures[].kind` field) should depend on this core
  * version instead.
  *
  * ==Boundary contract==
  *
  * This file compiles with zero `org.apache.spark.*` imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/field/MeasureKind.scala`
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 2 case objects, no behavior
  * - Equality auto-derived (case objects are singletons)
  * - Mirror of `io.semanticdf.MeasureKind` (identical shape, same comments)
  */
sealed trait MeasureKind
object MeasureKind {
  case object Base extends MeasureKind
  case object Calc extends MeasureKind
}