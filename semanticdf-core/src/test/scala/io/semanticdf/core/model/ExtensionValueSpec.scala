package io.semanticdf.core.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `ExtensionValue` is a usable, Spark-free
  * data record + the closed 6-variant enumeration. Per scala-data-
  * driven-refactor, this is pure data: the extension SHAPE is
  * engine-portable; the engine-specific compile is in the engine
  * adapter.
  */
class ExtensionValueSpec extends AnyFunSuite with Matchers {

  // -- 6 cases --

  test("Null is a singleton (canonical encoding for JSON null)") {
    ExtensionValue.Null shouldBe ExtensionValue.Null
  }

  test("String carries a String value") {
    ExtensionValue.String("hello").v shouldBe "hello"
  }

  test("Bool carries a Boolean value") {
    ExtensionValue.Bool(true).v shouldBe true
  }

  test("Number carries a BigDecimal value (lossless for JSON numbers)") {
    val n = BigDecimal("123.456")
    ExtensionValue.Number(n).v shouldBe n
  }

  test("List carries a List[ExtensionValue]") {
    val list = ExtensionValue.List(List(
      ExtensionValue.String("a"),
      ExtensionValue.Number(BigDecimal(1)),
    ))
    list.items.size shouldBe 2
  }

  test("Object carries a Map[String, ExtensionValue]") {
    val obj = ExtensionValue.Object(Map(
      "name" -> ExtensionValue.String("alice"),
      "age"  -> ExtensionValue.Number(BigDecimal(30)),
    ))
    obj.fields.size shouldBe 2
  }

  // -- closed enumeration --

  test("ExtensionValue has exactly 6 cases") {
    val all: Set[ExtensionValue] = Set(
      ExtensionValue.Null,
      ExtensionValue.String("a"),
      ExtensionValue.Bool(true),
      ExtensionValue.Number(BigDecimal(1)),
      ExtensionValue.List(List.empty),
      ExtensionValue.Object(Map.empty),
    )
    all.size shouldBe 6
  }

  test("Sealed exhaustiveness: pattern-match over all 6 cases") {
    val all: Seq[ExtensionValue] = Seq(
      ExtensionValue.Null,
      ExtensionValue.String("a"),
      ExtensionValue.Bool(true),
      ExtensionValue.Number(BigDecimal(1)),
      ExtensionValue.List(List.empty),
      ExtensionValue.Object(Map.empty),
    )
    all.foreach {
      case ExtensionValue.Null            => ()
      case _: ExtensionValue.String       => ()
      case _: ExtensionValue.Bool         => ()
      case _: ExtensionValue.Number       => ()
      case _: ExtensionValue.List         => ()
      case _: ExtensionValue.Object       => ()
    }
  }

  // -- recursive --

  test("nested List of List of Strings") {
    val nested = ExtensionValue.List(List(
      ExtensionValue.List(List(ExtensionValue.String("a"))),
      ExtensionValue.List(List(ExtensionValue.String("b"))),
    ))
    nested.items.size shouldBe 2
  }

  test("Object with nested Object") {
    val inner = ExtensionValue.Object(Map("x" -> ExtensionValue.String("y")))
    val outer = ExtensionValue.Object(Map("nested" -> inner))
    outer.fields("nested") shouldBe inner
  }

  // -- Null vs absence --

  test("Null is distinct from absence (per design DE finding 8.1)") {
    // A Map containing "key" -> Null is DIFFERENT from a Map without "key".
    val withNull: Map[String, ExtensionValue] = Map("key" -> ExtensionValue.Null)
    val withoutKey: Map[String, ExtensionValue] = Map.empty
    withNull.contains("key") shouldBe true
    withoutKey.contains("key") shouldBe false
  }

  // -- Serializable --

  test("all 6 cases round-trip through Java serialization (including nested)") {
    val cases: Seq[ExtensionValue] = Seq(
      ExtensionValue.Null,
      ExtensionValue.String("hello"),
      ExtensionValue.Bool(true),
      ExtensionValue.Number(BigDecimal("123.456")),
      ExtensionValue.List(List(ExtensionValue.String("a"), ExtensionValue.Number(BigDecimal(1)))),
      ExtensionValue.Object(Map("k" -> ExtensionValue.String("v"))),
    )
    cases.foreach { v =>
      val bos = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(bos)
      oos.writeObject(v)
      oos.close()
      val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
      val ois = new java.io.ObjectInputStream(bis)
      val restored = ois.readObject().asInstanceOf[ExtensionValue]
      restored shouldBe v
    }
  }
}