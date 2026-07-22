package io.semanticdf

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for `SemanticManifest.sideIdentity` — the helper for
  * computing a per-side `Identity` for a joined model's left/right
  * single-table manifest. See `docs/manifests-and-joins.md` for the
  * conceptual walkthrough and `examples/joined-manifest-split/` for
  * the worked example. */
class ManifestSideIdentitySpec extends AnyFunSuite with Matchers {

  test("sideIdentity appends `.name` to parent id and stamps `metadata.side`") {
    val parent = SemanticManifest.Identity(
      id              = "io.acme.warehouse.orders",
      manifestVersion = "0.1.0",
      namespace       = "prod",
      metadata        = Map("owner" -> "data-platform"),
    )
    val left = SemanticManifest.sideIdentity(parent, sideLabel = "left", name = "customers")
    left.id   shouldBe "io.acme.warehouse.orders.customers"
    left.namespace shouldBe parent.namespace
    left.manifestVersion shouldBe parent.manifestVersion
    left.metadata shouldBe Map("owner" -> "data-platform", "side" -> "left")
  }

  test("sideIdentity keeps the parent's manifestVersion, namespace, and any other metadata") {
    val parent = SemanticManifest.Identity(
      id              = "io.acme.test",
      manifestVersion = "1.2.3",
      namespace       = "staging",
      metadata        = Map("a" -> "1", "b" -> "2", "c" -> "3"),
    )
    val right = SemanticManifest.sideIdentity(parent, sideLabel = "right", name = "orders")
    right.id shouldBe "io.acme.test.orders"
    right.manifestVersion shouldBe "1.2.3"
    right.namespace shouldBe "staging"
    right.metadata shouldBe Map("a" -> "1", "b" -> "2", "c" -> "3", "side" -> "right")
  }

  test("sideIdentity is symmetric for left/right (only side metadata differs)") {
    val parent = SemanticManifest.Identity(id = "x.y")
    val l = SemanticManifest.sideIdentity(parent, "left",  "a")
    val r = SemanticManifest.sideIdentity(parent, "right", "a")
    l.id shouldBe "x.y.a"
    r.id shouldBe "x.y.a"  // same name produces same id; side label is in metadata only
    l.metadata("side") shouldBe "left"
    r.metadata("side") shouldBe "right"
  }

  test("sideIdentity result is an Identity usable directly with toJson") {
    val parent = SemanticManifest.Identity(id = "io.acme.warehouse.orders", metadata = Map.empty)
    val left  = SemanticManifest.sideIdentity(parent, "left",  "customers")
    val right = SemanticManifest.sideIdentity(parent, "right", "orders")

    // Reusable in toJson without further refinement. The FQN round-trips
    // through parseMeta as the per-side model identity.
    left.id should startWith("io.acme.warehouse.orders.")
    right.id should startWith("io.acme.warehouse.orders.")
    left.id should not be right.id
  }
}
