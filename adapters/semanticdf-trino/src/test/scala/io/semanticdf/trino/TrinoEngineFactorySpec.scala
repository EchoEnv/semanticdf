package io.semanticdf.trino

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 4: characterize the `TrinoEngine.withConnectionFactory`
  * invariant. These tests pin current behavior — they exist as
  * **characterization tests**, not as tests that assert an ideal.
  *
  * ==Why this spec exists==
  *
  * The `withConnectionFactory` method has a quirky API: it mutates
  * the engine and returns `this`. This is fine for fresh instances
  * (`new TrinoEngine().withConnectionFactory(...)`) but **not safe**
  * for the singleton `TrinoEngine.instance` — calling it on the
  * singleton would mutate shared state across all consumers.
  *
  * Rather than fixing the pre-existing bug (out of scope per
  * karpathy §3 — *don't fix things that aren't broken*), these
  * tests pin the current behavior so any future refactor that
  * changes it surfaces explicitly.
  *
  * ==Why a separate spec (not added to `TrinoEngineSpec`)==
  *
  * `TrinoEngineSpec` has 150+ tests focused on `compile` / `execute`
  * / `explain` (engine behavior). Adding "what does
  * `withConnectionFactory` do?" to that file dilutes its focus.
  * A separate small spec keeps each file's purpose clear: one
  * tests engine behavior, the other tests the fluent-API invariant.
  *
  * ==Per scala-data-driven-refactor (typed + data-driven + data-oriented)==
  *
  * The tests are *typed*: assertions on
  * `Option[() => TrinoConnection]` (no `Any`).
  *
  * They're *data-driven*: each test takes a small input set and
  * asserts on a deterministic output.
  *
  * They're *data-oriented*: no shared state; each test creates its
  * own engine. */
class TrinoEngineFactorySpec extends AnyFunSuite with Matchers {

  // -- factory assignment --

  test("a freshly-constructed engine has None as connectionFactory") {
    val engine = new TrinoEngine()
    engine.connectionFactory shouldBe None
  }

  test("withConnectionFactory sets a None factory to Some(f)") {
    val engine = new TrinoEngine()
    engine.connectionFactory shouldBe None  // default

    val factory: () => TrinoConnection = () =>
      null.asInstanceOf[TrinoConnection]
    val returned = engine.withConnectionFactory(factory)

    engine.connectionFactory shouldBe Some(factory)
    returned should be theSameInstanceAs(engine)
  }

  test("withConnectionFactory replaces an existing factory") {
    val engine = new TrinoEngine()
    val f1: () => TrinoConnection = () =>
      null.asInstanceOf[TrinoConnection]
    val f2: () => TrinoConnection = () =>
      null.asInstanceOf[TrinoConnection]

    engine.withConnectionFactory(f1)
    engine.connectionFactory shouldBe Some(f1)

    engine.withConnectionFactory(f2)
    engine.connectionFactory shouldBe Some(f2)  // f1 replaced, not appended
  }

  // -- isolation invariant --

  test("two engines hold independent factories") {
    val f1: () => TrinoConnection = () =>
      null.asInstanceOf[TrinoConnection]
    val f2: () => TrinoConnection = () =>
      null.asInstanceOf[TrinoConnection]

    val e1 = new TrinoEngine().withConnectionFactory(f1)
    val e2 = new TrinoEngine().withConnectionFactory(f2)

    e1.connectionFactory shouldBe Some(f1)
    e2.connectionFactory shouldBe Some(f2)
    e1.connectionFactory should not be theSameInstanceAs(e2.connectionFactory)
  }

  // -- fluent API contract --

  test("withConnectionFactory returns the same instance (fluent)") {
    // Documents the fluent-API contract: callers can chain
    //   val engine = new TrinoEngine().withConnectionFactory(...)
    // and the `this` reference is preserved (not a copy). This
    // also documents the *implicit* mutation behavior — if a
    // future refactor moves to immutable builders, this test
    // breaks and the change is forced to be intentional.
    val engine = new TrinoEngine()
    val f1: () => TrinoConnection = () =>
      null.asInstanceOf[TrinoConnection]
    val returned = engine.withConnectionFactory(f1)
    returned should be theSameInstanceAs(engine)
  }
}
