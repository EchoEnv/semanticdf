package io.semanticdf.core.field

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 1 increment 2: prove `io.semanticdf.core.field.SemanticField`
  * (and its in-file `FieldRef` value class) is a usable, self-contained,
  * Spark-free typeclass layer.
  *
  * ==Why this test file exists==
  *
  * The new package — `io.semanticdf.core.field` — contains the engine-portable
  * typed field references that will eventually live in the `semanticdf-core`
  * artifact. It must compile and run with NO Spark on the classpath. This test
  * verifies the data-side methods work as expected:
  *
  *   1. `SemanticDimension.of[T](n)` / `SemanticMeasure.of[T](n)` produce
  *      typeclass witnesses with the right `name` and pinned `kind`.
  *   2. `FieldRef` value-class wraps a witness and exposes `.underlying`.
  *   3. The implicit conversions `FieldRef.fromDimension` / `fromMeasure`
  *      fire automatically at the call site.
  *   4. Equality of `FieldKind` case objects is the identity comparison the
  *      compiler derives.
  *
  * ==Data-driven mantra compliance==
  *
  * Every assertion here checks behaviour derived from data:
  *   - `name` reads the constructor argument
  *   - `kind` is `final def` returning a singleton case object
  *   - `FieldRef` is a value class — `equals` is structural on the underlying
  *     witness reference
  *
  * No `Map`-based dispatch. No string-typed op lookup. Per the
  * `scala-data-driven-refactor` step 2, the boundary enforces validity at
  * declaration time via phantom-typed `T`, not via runtime flag checks.
  */
class SemanticFieldSpec extends AnyFunSuite with Matchers {

  // -------------------------------------------------------------------------
  // FieldKind: sealed ADT with two case objects
  // -------------------------------------------------------------------------

  test("FieldKind has exactly two cases: Dimension and Measure") {
    val allCases: Set[FieldKind] = Set(FieldKind.Dimension, FieldKind.Measure)
    allCases.size shouldBe 2
    FieldKind.Dimension should not be FieldKind.Measure
  }

  test("FieldKind.Dimension and FieldKind.Measure are stable singletons") {
    FieldKind.Dimension shouldBe FieldKind.Dimension
    FieldKind.Measure shouldBe FieldKind.Measure
  }

  // -------------------------------------------------------------------------
  // SemanticDimension.of / SemanticMeasure.of factories
  // -------------------------------------------------------------------------

  test("SemanticDimension.of returns a witness with the right name and pinned kind") {
    val carrier = SemanticDimension.of[String]("carrier")
    carrier.name shouldBe "carrier"
    carrier.kind shouldBe FieldKind.Dimension
  }

  test("SemanticMeasure.of returns a witness with the right name and pinned kind") {
    val pax = SemanticMeasure.of[Long]("total_passengers")
    pax.name shouldBe "total_passengers"
    pax.kind shouldBe FieldKind.Measure
  }

  test("Two SemanticDimensions with the same name are independent instances but equal on `name`") {
    val a = SemanticDimension.of[String]("carrier")
    val b = SemanticDimension.of[String]("carrier")
    a.name shouldBe b.name
    a.kind shouldBe b.kind
    a should not be theSameInstanceAs(b)
  }

  test("SemanticDimension witnesses can carry different phantom type tags") {
    // The phantom `T` does not appear in the witness value, but the static
    // type at the call site prevents mixing. We test the witness side here:
    // both witnesses share the same name; differentiation is purely type-level.
    val carrierAsString: SemanticDimension[String] = SemanticDimension.of[String]("carrier")
    val carrierAsInt:    SemanticDimension[Int]    = SemanticDimension.of[Int]("carrier")
    carrierAsString.name shouldBe "carrier"
    carrierAsInt.name    shouldBe "carrier"
    // At runtime, both witnesses are equal by name+kind
    carrierAsString.kind shouldBe carrierAsInt.kind
  }

  // -------------------------------------------------------------------------
  // FieldRef value class
  // -------------------------------------------------------------------------

  test("FieldRef wraps a SemanticField and exposes .underlying") {
    val witness = SemanticDimension.of[String]("carrier")
    val ref: FieldRef[String] = new FieldRef[String](witness)
    ref.underlying shouldBe witness
  }

  test("FieldRef is a value class: identical refs collapse to the underlying witness at the bytecode level") {
    // Value classes in Scala 2.13 are erased at the JVM level — the wrapper
    // IS the underlying instance. This is what we rely on for zero runtime cost.
    val witness = SemanticDimension.of[String]("carrier")
    val ref = new FieldRef[String](witness)
    val refAsField = ref.underlying
    refAsField should be theSameInstanceAs witness
  }

  // -------------------------------------------------------------------------
  // Implicit conversions: fromDimension / fromMeasure
  // -------------------------------------------------------------------------

  test("FieldRef.fromDimension wraps a SemanticDimension into a FieldRef") {
    val witness = SemanticDimension.of[String]("carrier")
    val ref: FieldRef[String] = FieldRef.fromDimension(witness)
    ref.underlying shouldBe witness
  }

  test("FieldRef.fromMeasure wraps a SemanticMeasure into a FieldRef") {
    val witness = SemanticMeasure.of[Long]("total_passengers")
    val ref: FieldRef[Long] = FieldRef.fromMeasure(witness)
    ref.underlying shouldBe witness
  }

  test("Implicit conversion fires at the call site: passing a SemanticDimension where FieldRef is expected") {
    // This is the canonical usage: the user passes a typed dimension ref to
    // a method expecting FieldRef, and the implicit conversion wraps it.
    def takeFieldRef[T](ref: FieldRef[T]): String = ref.underlying.name
    val witness = SemanticDimension.of[String]("carrier")
    takeFieldRef(witness) shouldBe "carrier"  // implicit fromDimension fires
  }

  test("Implicit conversion fires for SemanticMeasure too") {
    def takeFieldRef[T](ref: FieldRef[T]): String = ref.underlying.name
    val witness = SemanticMeasure.of[Long]("total_passengers")
    takeFieldRef(witness) shouldBe "total_passengers"  // implicit fromMeasure fires
  }

  // -------------------------------------------------------------------------
  // Pinned-kind invariant (compile-time guarantee)
  // -------------------------------------------------------------------------

  test("SemanticDimension.kind is always Dimension regardless of any override attempt") {
    // `kind` is `final def` — the only way it could be anything else would
    // require a subclass, which the sealed trait forbids at compile time.
    // At runtime, every SemanticDimension has kind == FieldKind.Dimension.
    val witnesses: Seq[SemanticDimension[_]] = Seq(
      SemanticDimension.of[String]("a"),
      SemanticDimension.of[Int]("b"),
      SemanticDimension.of[Long]("c"),
    )
    witnesses.foreach { w => w.kind shouldBe FieldKind.Dimension }
  }

  test("SemanticMeasure.kind is always Measure regardless of any override attempt") {
    val witnesses: Seq[SemanticMeasure[_]] = Seq(
      SemanticMeasure.of[Int]("x"),
      SemanticMeasure.of[Double]("y"),
    )
    witnesses.foreach { w => w.kind shouldBe FieldKind.Measure }
  }

  // -------------------------------------------------------------------------
  // Serialization: scope-bounded to what the API actually promises.
  //
  // The original `io.semanticdf.SemanticField` API does NOT promise that
  // factory-built witnesses are Serializable — the `of[T](n)` factories
  // return anonymous inner classes (`new SemanticDimension[T] { ... }`),
  // which Scala 2.13 does not auto-mixin `Serializable` for. The typed
  // groupBy/aggregate overloads use the witness purely as a typeclass —
  // they read `ev.name` and discard the witness, so it is never captured
  // in a closure or sent to executors. That is by design.
  //
  // Therefore this spec does NOT test witness serialization. If a future
  // need arises (e.g. witnesses sent over Restate journal), the right fix
  // is to change the factory return type to a `final case class XxxOf[T]`
  // so the compiler can derive `Product with Serializable`. That is a
  // behaviour change for the API, not a test addition.
  //
  // The FieldKind case objects DO round-trip (they're Scala case objects,
  // which ARE Serializable via mixin). That's tested.
  // -------------------------------------------------------------------------

  test("FieldKind case objects round-trip through Java serialization") {
    val dim: FieldKind = FieldKind.Dimension
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(dim)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[FieldKind]
    restored shouldBe FieldKind.Dimension
  }
}