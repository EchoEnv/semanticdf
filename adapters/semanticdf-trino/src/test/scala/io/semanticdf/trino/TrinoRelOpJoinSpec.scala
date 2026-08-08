package io.semanticdf.trino

import io.semanticdf.core.engine.ResolvedSource
import io.semanticdf.core.model.SourceRef
import io.semanticdf.core.engine.EngineError
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.rel.{JoinKind, RelOp}
import io.semanticdf.core.engine.ResolvedSchema
import io.semanticdf.core.schema.{Field, SealedDataType}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for [[TrinoQueryCompiler.compileRelOp]]'s `RelOp.Join` case
 * (v0.3.1 Gap 3 closure).
 *
 * Per the v0.3.1 backlog: "Extends `compileRelOp(plan, modelSources)`
 * signature (both Trino + DuckDB)." — closes the deferred portion of
 * Gap 3 so hand-built `RelOp` plans with joins also work end-to-end.
 *
 * Per scala-spark-batch-bugs §1: assert the actual SQL output, not
 * just compile success.
 */
class TrinoRelOpJoinSpec extends AnyFunSuite with Matchers {

  private val compiler = TrinoQueryCompiler.instance

  /** Source for the "orders" side of the join. */
  private def ordersSource: SourceRef.ByName = SourceRef.ByName(
    catalog = Some("hive"), namespace = Some("silver"), table = "orders",
  )

  /** Source for the "customers" side. */
  private def customersSource: SourceRef.ByName = SourceRef.ByName(
    catalog = Some("hive"), namespace = Some("silver"), table = "customers",
  )

  /** Source-resolution map passed to compileRelOp (new signature). */
  private def sources: Map[String, SourceRef] = Map(
    "orders" -> ordersSource,
    "customers" -> customersSource,
  )

  test("compileRelOp returns Right with JOIN clause for a Join RelOp") {
    val plan = RelOp.Join(
      left   = RelOp.Scan(ResolvedSource.Scan(ordersSource, ResolvedSchema(Map.empty)), Nil, Nil),
      right  = RelOp.Scan(ResolvedSource.Scan(customersSource, ResolvedSchema(Map.empty)), Nil, Nil),
      kind   = JoinKind.Inner,
      condition = Expr.Equal(Expr.FieldRef("id"), Expr.FieldRef("id")),
    )
    val result = compiler.compileRelOp(plan, sources)
    result match {
      case Right(sql) =>
        sql.sql should include ("INNER JOIN")
        sql.sql should include ("\"hive\".\"silver\".\"customers\"")
        sql.sql should include ("\"hive\".\"silver\".\"orders\"")
      case Left(err) =>
        fail(s"expected Right with JOIN clause, got Left($err)")
    }
  }

  test("compileRelOp handles JoinKind.Left (LEFT JOIN)") {
    val plan = RelOp.Join(
      left   = RelOp.Scan(ResolvedSource.Scan(ordersSource, ResolvedSchema(Map.empty)), Nil, Nil),
      right  = RelOp.Scan(ResolvedSource.Scan(customersSource, ResolvedSchema(Map.empty)), Nil, Nil),
      kind   = JoinKind.Left,
      condition = Expr.Equal(Expr.FieldRef("id"), Expr.FieldRef("id")),
    )
    val result = compiler.compileRelOp(plan, sources)
    result match {
      case Right(sql) => sql.sql should include ("LEFT JOIN")
      case Left(err)  => fail(s"expected Right, got Left($err)")
    }
  }

  test("compileRelOp handles JoinKind.Cross (CROSS JOIN, no ON clause)") {
    val plan = RelOp.Join(
      left   = RelOp.Scan(ResolvedSource.Scan(ordersSource, ResolvedSchema(Map.empty)), Nil, Nil),
      right  = RelOp.Scan(ResolvedSource.Scan(customersSource, ResolvedSchema(Map.empty)), Nil, Nil),
      kind   = JoinKind.Cross,
      condition = Expr.Equal(Expr.FieldRef("id"), Expr.FieldRef("id")),
    )
    val result = compiler.compileRelOp(plan, sources)
    result match {
      case Right(sql) =>
        // Cross joins have no ON clause (SQL syntax).
        sql.sql should include ("CROSS JOIN")
        sql.sql should not include (" ON ")
      case Left(err) => fail(s"expected Right, got Left($err)")
    }
  }

  test("compileRelOp fails loud if a Join references a table not in sources map") {
    // Per the standard (error-handling-style.md): typed Either, no
    // silent fallback. Unknown right-side source is a programmer
    // error at the boundary.
    val plan = RelOp.Join(
      left   = RelOp.Scan(ResolvedSource.Scan(ordersSource, ResolvedSchema(Map.empty)), Nil, Nil),
      right  = RelOp.Scan(ResolvedSource.Scan(customersSource, ResolvedSchema(Map.empty)), Nil, Nil),
      kind   = JoinKind.Inner,
      condition = Expr.Equal(Expr.FieldRef("id"), Expr.FieldRef("id")),
    )
    val unknownSource = SourceRef.ByName(catalog = Some("hive"), namespace = Some("silver"), table = "orphan")
    val plan2 = plan.copy(right = RelOp.Scan(ResolvedSource.Scan(unknownSource, ResolvedSchema(Map.empty)), Nil, Nil))
    val result = compiler.compileRelOp(plan2, sources)  // orphan not in modelSources map
    result match {
      case Left(_: EngineError.UnsupportedCapability) => // expected
      case Left(other) => fail(s"expected UnsupportedCapability, got Left($other)")
      case Right(sql)  => fail(s"expected Left, got Right($sql)")
    }
  }
}
