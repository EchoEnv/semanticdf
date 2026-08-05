package io.semanticdf.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.LiteralValue

/** Phase 2 contract: prove `ParameterizedSql` is a pure-data case
  * class with a Serializable round-trip.
  *
  * Per scala-data-driven-refactor §1: the shape is data; the engine
  * adapter does the binding behavior.
  */
class ParameterizedSqlSpec extends AnyFunSuite with Matchers {

  // -- case class shape --

  test("ParameterizedSql has sql + parameters fields") {
    val psql = ParameterizedSql(
      sql        = """SELECT * FROM "orders" WHERE "id" = ?""",
      parameters = List(LiteralValue.IntValue(42)),
    )
    psql.sql shouldBe """SELECT * FROM "orders" WHERE "id" = ?"""
    psql.parameters shouldBe List(LiteralValue.IntValue(42))
  }

  test("parameterCount returns the number of parameters") {
    val psql = ParameterizedSql(
      sql        = "SELECT * FROM t WHERE a = ? AND b = ?",
      parameters = List(
        LiteralValue.IntValue(1),
        LiteralValue.StringValue("x"),
      ),
    )
    psql.parameterCount shouldBe 2
  }

  test("parameterCount is 0 for empty parameters list") {
    val psql = ParameterizedSql(
      sql        = "SELECT * FROM t",
      parameters = Nil,
    )
    psql.parameterCount shouldBe 0
  }

  // -- equality / structural --

  test("two ParameterizedSql with the same content are equal") {
    val a = ParameterizedSql("SELECT 1", Nil)
    val b = ParameterizedSql("SELECT 1", Nil)
    a shouldBe b
  }

  test("two ParameterizedSql with different parameters are NOT equal") {
    val a = ParameterizedSql("SELECT ?", List(LiteralValue.IntValue(1)))
    val b = ParameterizedSql("SELECT ?", List(LiteralValue.IntValue(2)))
    a should not be b
  }

  test("two ParameterizedSql with different sql are NOT equal") {
    val a = ParameterizedSql("SELECT 1", Nil)
    val b = ParameterizedSql("SELECT 2", Nil)
    a should not be b
  }

  // -- Serializable round-trip --

  test("ParameterizedSql round-trips through Java serialization") {
    val original = ParameterizedSql(
      sql        = """SELECT "amount" FROM "orders" WHERE "id" = ? AND "region" = ?""",
      parameters = List(
        LiteralValue.LongValue(42L),
        LiteralValue.StringValue("AA"),
      ),
    )
    val bytes = serialize(original)
    val restored = deserialize[ParameterizedSql](bytes)
    restored shouldBe original
  }

  // -- helpers --

  /** Serialize a value using Java serialization (the same path
    * `Serializable` types use). */
  private def serialize[T <: Serializable](value: T): Array[Byte] = {
    val baos = new java.io.ByteArrayOutputStream()
    val oos  = new java.io.ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.close()
    baos.toByteArray
  }

  /** Deserialize a value using Java serialization. */
  private def deserialize[T](bytes: Array[Byte]): T = {
    val bais = new java.io.ByteArrayInputStream(bytes)
    val ois  = new java.io.ObjectInputStream(bais)
    ois.readObject().asInstanceOf[T]
  }
}