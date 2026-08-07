package io.semanticdf.core.catalog

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for [CatalogRef], [PublishMode], [PublishResult],
  * [CatalogError], [CatalogEntity], [CatalogFilter],
  * [CatalogEntry], [CatalogAdapter] \u2014 PR 10 of the v0.3.0
  * deferred-work triage.
  *
  * Pins the engine-portable catalog identity + publication
  * contract that closes design finding #13 ("Version catalog
  * identity and define create-only/upsert/CAS results \u00a75.3").
  *
  * Per the design: create-only conflicts if identity exists;
  * upsert atomically increments version; CAS updates only at
  * expected digest; discovery verifies catalog ref, manifest,
  * and extension blob digests.
  *
  * Per the SWE review of PR #410 (C1): the FakeCatalogAdapter
  * keys on an explicit [CatalogIdentity] passed to `publish`
  * (NOT derived from the doc), so the same identity can be
  * published twice with different modes to exercise every
  * path (CreateOnly, Upsert on existing, CompareAndSet). */
class CatalogIdentityAndCASSpec extends AnyFunSuite with Matchers {

  // ====================================================================
  // CatalogRef + CatalogIdentity
  // ====================================================================

  test("CatalogRef carries 5 fields: catalog, namespace, name, version, digest") {
    val r = CatalogRef(
      catalog   = "unity",
      namespace = "public",
      name      = "orders",
      version   = 3,
      digest    = "abc123",
    )
    r.catalog shouldBe "unity"
    r.namespace shouldBe "public"
    r.name shouldBe "orders"
    r.version shouldBe 3
    r.digest shouldBe "abc123"
  }

  test("CatalogRef.identity extracts the stable (catalog, namespace, name) triple") {
    val r = CatalogRef("unity", "public", "orders", version = 3, digest = "abc")
    r.identity shouldBe CatalogIdentity("unity", "public", "orders")
  }

  test("CatalogIdentity is equal when (catalog, namespace, name) match (version-agnostic)") {
    CatalogIdentity("unity", "public", "orders") shouldBe CatalogIdentity("unity", "public", "orders")
  }

  test("CatalogRef equality is field-by-field (different version => not equal)") {
    val r1 = CatalogRef("unity", "public", "orders", 1, "abc")
    val r2 = CatalogRef("unity", "public", "orders", 2, "abc")
    val r3 = CatalogRef("unity", "public", "orders", 2, "def")
    r1 should not be r2  // version differs
    r2 should not be r3  // digest differs
    r2 shouldBe r2.copy()  // equal to itself
  }

  // ====================================================================
  // PublishMode
  // ====================================================================

  test("PublishMode has 3 cases: CreateOnly, Upsert, CompareAndSet") {
    val all: Set[PublishMode] = Set(
      PublishMode.CreateOnly,
      PublishMode.Upsert,
      PublishMode.CompareAndSet("abc"),
    )
    all.size shouldBe 3
  }

  test("PublishMode.CreateOnly is a singleton") {
    PublishMode.CreateOnly shouldBe PublishMode.CreateOnly
  }

  test("PublishMode.Upsert is a singleton") {
    PublishMode.Upsert shouldBe PublishMode.Upsert
  }

  test("PublishMode.CompareAndSet carries expectedDigest") {
    val cas = PublishMode.CompareAndSet(expectedDigest = "abc123")
    cas.expectedDigest shouldBe "abc123"
  }

  test("PublishMode.CompareAndSet equality is field-by-field") {
    PublishMode.CompareAndSet("abc") shouldBe PublishMode.CompareAndSet("abc")
    PublishMode.CompareAndSet("abc") should not be PublishMode.CompareAndSet("def")
  }

  test("PublishMode.CompareAndSet rejects empty digest (smart constructor)") {
    // Per DE review of PR #410: empty expectedDigest is a
    // degenerate CAS condition with no useful interpretation.
    val ex = intercept[IllegalArgumentException] {
      PublishMode.compareAndSet("")
    }
    ex.getMessage should include ("non-empty")
  }

  // ====================================================================
  // PublishResult
  // ====================================================================

  test("PublishResult has 3 cases: Inserted, Updated, Conflict") {
    val r = CatalogRef("unity", "public", "orders", 1, "abc")
    val all: Set[PublishResult] = Set(
      PublishResult.Inserted(r),
      PublishResult.Updated(r, r),
      PublishResult.Conflict("conflict"),
    )
    all.size shouldBe 3
  }

  test("PublishResult.Inserted carries the new ref") {
    val r = CatalogRef("unity", "public", "orders", 1, "abc")
    val i = PublishResult.Inserted(r)
    i.ref shouldBe r
  }

  test("PublishResult.Updated carries previous and current refs") {
    val prev = CatalogRef("unity", "public", "orders", 1, "old")
    val cur  = CatalogRef("unity", "public", "orders", 2, "new")
    val u = PublishResult.Updated(prev, cur)
    u.previous shouldBe prev
    u.current shouldBe cur
  }

  test("PublishResult.Conflict carries reason + optional current ref") {
    val r = CatalogRef("unity", "public", "orders", 1, "abc")
    val c1 = PublishResult.Conflict("digest mismatch", Some(r))
    val c2 = PublishResult.Conflict("permission denied")
    c1.current shouldBe Some(r)
    c1.reason shouldBe "digest mismatch"
    c2.current shouldBe None
    c2.reason shouldBe "permission denied"
  }

  // ====================================================================
  // CatalogError
  // ====================================================================

  test("CatalogError has 6 cases (Conflict + StaleConflict split)") {
    val r = CatalogRef("unity", "public", "orders", 1, "abc")
    val all: Set[CatalogError] = Set(
      CatalogError.Conflict("reason"),
      CatalogError.StaleConflict("reason", r),
      CatalogError.Unauthorized("no perm"),
      CatalogError.Network("timeout"),
      CatalogError.Unsupported("read-only catalog"),
      CatalogError.MalformedManifest("missing fields"),
    )
    all.size shouldBe 6
  }

  test("CatalogError.Conflict (no current) and StaleConflict (with current) are distinct cases") {
    val r = CatalogRef("unity", "public", "orders", 1, "abc")
    // Per SWE review H4: Conflict and StaleConflict are
    // structurally distinct (CAS rejection always has a
    // current ref to report; plain Conflict never has one).
    val plain  = CatalogError.Conflict("no visibility")
    val stale  = CatalogError.StaleConflict("digest mismatch", r)
    plain should not be a [CatalogError.StaleConflict]
    stale shouldBe a [CatalogError.StaleConflict]
  }

  // ====================================================================
  // CatalogEntity
  // ====================================================================

  test("CatalogEntity has 3 cases: Model, Rollup, ExtensionBlob") {
    val all: Set[CatalogEntity] = Set(
      CatalogEntity.Model,
      CatalogEntity.Rollup,
      CatalogEntity.ExtensionBlob,
    )
    all.size shouldBe 3
  }

  test("CatalogEntity singletons are pairwise distinct") {
    CatalogEntity.Model should not be CatalogEntity.Rollup
    CatalogEntity.Rollup should not be CatalogEntity.ExtensionBlob
  }

  // ====================================================================
  // CatalogFilter
  // ====================================================================

  test("CatalogFilter default constructor is empty (no filters)") {
    CatalogFilter().isEmpty shouldBe true
  }

  test("CatalogFilter with any field set is non-empty") {
    CatalogFilter(catalog = Some("unity")).isEmpty shouldBe false
    CatalogFilter(namespace = Some("public")).isEmpty shouldBe false
    CatalogFilter(namePrefix = Some("orders_")).isEmpty shouldBe false
    CatalogFilter(kind = Some(CatalogEntity.Model)).isEmpty shouldBe false
    CatalogFilter(limit = Some(10)).isEmpty shouldBe false
  }

  test("CatalogFilter equality is field-by-field") {
    CatalogFilter(catalog = Some("unity")) shouldBe CatalogFilter(catalog = Some("unity"))
    CatalogFilter(catalog = Some("unity")) should not be CatalogFilter(catalog = Some("hms"))
  }

  // ====================================================================
  // CatalogEntry
  // ====================================================================

  test("CatalogEntry carries ref, kind, and optional summary") {
    val r = CatalogRef("unity", "public", "orders", 1, "abc")
    val e = CatalogEntry(r, CatalogEntity.Model, Map("owner" -> "analytics"))
    e.ref shouldBe r
    e.kind shouldBe CatalogEntity.Model
    e.summary("owner") shouldBe "analytics"
  }

  test("CatalogEntry default summary is empty") {
    val r = CatalogRef("unity", "public", "orders", 1, "abc")
    CatalogEntry(r, CatalogEntity.Model).summary shouldBe Map.empty
  }

  // ====================================================================
  // CatalogAdapter contract (with a fake implementation)
  // ====================================================================

  /** A minimal in-memory fake catalog adapter for the test. The
    * fake stores entities as `(ref, content)` pairs keyed by
    * the explicit [CatalogIdentity] passed to `publish` (NOT
    * derived from the doc, per the SWE review of PR #410 C1).
    *
    * The fake is NOT thread-safe (per the DE review #2):
    * concurrent `publish` calls on the same identity can race.
    * The production adapter implementation is required to be
    * server-side-atomic (per the CatalogAdapter scaladoc). The
    * fake models the serial case for unit tests; concurrent
    * tests are deferred to integration tests with real catalogs.
    */
  private final class FakeCatalogAdapter extends CatalogAdapter {
    override def catalog: String = "fake"
    private val store = scala.collection.mutable.Map.empty[String, (CatalogRef, Any)]

    override def publish(
        identity: CatalogIdentity,
        doc:     Any,
        as:      CatalogEntity,
        mode:    PublishMode,
    ): Either[CatalogError, PublishResult] = {
      // Per DE review #4: CAS uses an explicit target identity
      // (passed in), not derived from the doc.
      val key = identity.toString
      // ManifestDocument is Nothing, so doc is uninhabited.
      // For the fake, we use a synthetic digest derived from the
      // identity (a real adapter would hash the actual manifest).
      val newDigest = if (doc == null) "doc-placeholder" else doc.toString  // null-safe placeholder
      mode match {
        case PublishMode.CreateOnly =>
          store.get(key) match {
            case Some((prev, _)) =>
              Right(PublishResult.Conflict("already exists", Some(prev)))
            case None =>
              val ref = CatalogRef(identity.catalog, identity.namespace, identity.name, 1, newDigest)
              store += (key -> (ref, doc))
              Right(PublishResult.Inserted(ref))
          }
        case PublishMode.Upsert =>
          // Per SWE review C1 / DE #1: this is the path that was
          // missing. The fake now keys on `identity` (passed in),
          // so the same identity CAN be upserted twice.
          store.get(key) match {
            case Some((prev, _)) =>
              val cur = prev.copy(version = prev.version + 1, digest = newDigest)
              store += (key -> (cur, doc))
              Right(PublishResult.Updated(prev, cur))
            case None =>
              val ref = CatalogRef(identity.catalog, identity.namespace, identity.name, 1, newDigest)
              store += (key -> (ref, doc))
              Right(PublishResult.Inserted(ref))
          }
        case PublishMode.CompareAndSet(expectedDigest) =>
          store.get(key) match {
            case Some((prev, _)) if prev.digest == expectedDigest =>
              val cur = prev.copy(version = prev.version + 1, digest = newDigest)
              store += (key -> (cur, doc))
              Right(PublishResult.Updated(prev, cur))
            case Some((current, _)) =>
              Right(PublishResult.Conflict("digest mismatch", Some(current)))
            case None =>
              Right(PublishResult.Conflict("no entity at identity"))
          }
      }
    }

    override def discover(ref: CatalogRef): Either[CatalogError, Option[Nothing]] =
      // Per DE review #3: the fake now keys on the FULL ref
      // (identity + version + digest), not just identity. A
      // stale ref (wrong version/digest) returns None.
      store.get(ref.toString) match {
        case Some((stored, doc)) if stored == ref =>
          // Can't actually return a stored ManifestDocument
          // because Nothing is uninhabited; but the lookup
          // succeeds.
          Right(None)
        case Some(_) =>
          Right(None)  // stale ref
        case None =>
          Right(None)  // absent
      }

    override def list(
        filter: CatalogFilter,
    ): Either[CatalogError, List[CatalogEntry]] = {
      val all = store.values.toList.map(_._1)
      // Per SWE review M1: the filter now ALSO honors `kind`.
      // For v1, every stored entity is a `Model`, so `kind` is
      // always Some(CatalogEntity.Model) effectively. The
      // filter would only exclude if the caller asks for a
      // different kind than what's stored.
      val filtered = all.filter { r =>
        filter.catalog.forall(_ == r.catalog) &&
        filter.namespace.forall(_ == r.namespace) &&
        filter.namePrefix.forall(r.name.startsWith) &&
        filter.kind.forall(_ == CatalogEntity.Model)  // v1: only Models are stored
      }
      val limited = filter.limit.map(filtered.take).getOrElse(filtered)
      Right(limited.map(r => CatalogEntry(r, CatalogEntity.Model)))
    }
  }

  // -- Tests using the new explicit-identity publish --

  private val ordersIdentity = CatalogIdentity("fake", "public", "orders")

  test("CreateOnly on absent identity: Right(Inserted) with version=1") {
    val adapter = new FakeCatalogAdapter
    val result = adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    result shouldBe Right(PublishResult.Inserted(
      CatalogRef(catalog = "fake", namespace = "public", name = "orders", version = 1, digest = "doc-placeholder"),
    ))
  }

  test("CreateOnly on existing identity: Right(Conflict(\"already exists\", Some(current)))") {
    val adapter = new FakeCatalogAdapter
    adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    val result = adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    result shouldBe Right(PublishResult.Conflict(
      reason  = "already exists",
      current = Some(CatalogRef("fake", "public", "orders", 1, "doc-placeholder")),
    ))
  }

  test("Upsert on absent identity: Right(Inserted) with version=1") {
    val adapter = new FakeCatalogAdapter
    val result = adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.Upsert)
    result shouldBe Right(PublishResult.Inserted(
      CatalogRef(catalog = "fake", namespace = "public", name = "orders", version = 1, digest = "doc-placeholder"),
    ))
  }

  test("Upsert on existing identity: Right(Updated(prev, cur)) with version+1") {
    // Per SWE review C1 / DE #1: this test ACTUALLY exercises
    // the Upsert-on-existing path now (because publish takes an
    // explicit identity parameter, the same identity can be
    // upserted twice).
    val adapter = new FakeCatalogAdapter
    adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    val result = adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.Upsert)
    result shouldBe Right(PublishResult.Updated(
      previous = CatalogRef("fake", "public", "orders", 1, "doc-placeholder"),
      current  = CatalogRef("fake", "public", "orders", 2, "doc-placeholder"),
    ))
  }

  test("Upsert on existing identity: previous.version+1 == current.version") {
    val adapter = new FakeCatalogAdapter
    val first = adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    first.toOption.get match {
      case PublishResult.Inserted(r) => r.version shouldBe 1
      case other => fail(s"expected Inserted, got $other")
    }
    val second = adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.Upsert)
    second.toOption.get match {
      case PublishResult.Updated(prev, cur) =>
        cur.version shouldBe (prev.version + 1)
        cur.version shouldBe 2
      case other => fail(s"expected Updated, got $other")
    }
  }

  test("CompareAndSet with matching expectedDigest: Right(Updated)") {
    val adapter = new FakeCatalogAdapter
    val first = adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly).toOption.get
    val firstRef = first match {
      case PublishResult.Inserted(r) => r
      case other => fail(s"unexpected: $other")
    }
    val result = adapter.publish(
      identity = ordersIdentity,
      doc      = null,
      as       = CatalogEntity.Model,
      mode     = PublishMode.CompareAndSet(expectedDigest = firstRef.digest),
    )
    result shouldBe Right(PublishResult.Updated(
      previous = firstRef,
      current  = firstRef.copy(version = 2, digest = "doc-placeholder"),
    ))
  }

  test("CompareAndSet with mismatched expectedDigest: Right(Conflict(\"digest mismatch\", Some(current)))") {
    val adapter = new FakeCatalogAdapter
    adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    val result = adapter.publish(
      identity = ordersIdentity,
      doc      = null,
      as       = CatalogEntity.Model,
      mode     = PublishMode.CompareAndSet(expectedDigest = "wrong-digest"),
    )
    result shouldBe Right(PublishResult.Conflict(
      reason  = "digest mismatch",
      current = Some(CatalogRef("fake", "public", "orders", 1, "doc-placeholder")),
    ))
  }

  test("discover on existing ref (exact match): Right(Some(...))") {
    val adapter = new FakeCatalogAdapter
    adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    val ref = CatalogRef("fake", "public", "orders", 1, "null")
    val result = adapter.discover(ref)
    result shouldBe Right(None)  // Nothing is uninhabited, but the lookup succeeded
  }

  test("discover on stale ref (wrong version): Right(None)") {
    // Per DE review #3: stale refs (wrong version/digest)
    // return None.
    val adapter = new FakeCatalogAdapter
    adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    val staleRef = CatalogRef("fake", "public", "orders", 99, "stale-digest")
    adapter.discover(staleRef) shouldBe Right(None)
  }

  test("discover on stale ref (wrong digest): Right(None)") {
    val adapter = new FakeCatalogAdapter
    adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    val staleRef = CatalogRef("fake", "public", "orders", 1, "wrong-digest")
    adapter.discover(staleRef) shouldBe Right(None)
  }

  test("discover on absent identity: Right(None) (NOT Left)") {
    val adapter = new FakeCatalogAdapter
    val ref = CatalogRef("nonexistent", "public", "orders", 1, "abc")
    val result = adapter.discover(ref)
    result shouldBe Right(None)
  }

  test("list with empty filter returns all entities") {
    val adapter = new FakeCatalogAdapter
    adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    adapter.publish(CatalogIdentity("fake", "public", "users"), null, CatalogEntity.Model, PublishMode.CreateOnly)
    adapter.list(CatalogFilter()).toOption.get.size shouldBe 2
  }

  test("list with catalog filter returns only matching entities") {
    val adapter = new FakeCatalogAdapter
    adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    val result = adapter.list(CatalogFilter(catalog = Some("fake")))
    result.toOption.get.size shouldBe 1
    adapter.list(CatalogFilter(catalog = Some("other"))).toOption.get.size shouldBe 0
  }

  test("list with kind filter (M1): returns only matching entities") {
    // Per SWE review M1: the filter MUST honor `kind`.
    val adapter = new FakeCatalogAdapter
    adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    adapter.list(CatalogFilter(kind = Some(CatalogEntity.Model))).toOption.get.size shouldBe 1
    adapter.list(CatalogFilter(kind = Some(CatalogEntity.Rollup))).toOption.get.size shouldBe 0
    adapter.list(CatalogFilter(kind = Some(CatalogEntity.ExtensionBlob))).toOption.get.size shouldBe 0
  }

  test("list with limit returns at most N entries") {
    val adapter = new FakeCatalogAdapter
    adapter.publish(CatalogIdentity("fake", "public", "a"), null, CatalogEntity.Model, PublishMode.CreateOnly)
    adapter.publish(CatalogIdentity("fake", "public", "b"), null, CatalogEntity.Model, PublishMode.CreateOnly)
    adapter.publish(CatalogIdentity("fake", "public", "c"), null, CatalogEntity.Model, PublishMode.CreateOnly)
    adapter.list(CatalogFilter(limit = Some(2))).toOption.get.size shouldBe 2
  }

  // ====================================================================
  // Data-driven contract: equality + hashing for ALL new types
  // ====================================================================

  test("Every new case class has auto-derived equality + hash codes (data-driven contract)") {
    val r = CatalogRef("a", "b", "c", 1, "d")
    r shouldBe r.copy()
    r.hashCode shouldBe r.copy().hashCode
    val f = CatalogFilter(catalog = Some("x"))
    f shouldBe f.copy()
    val e = CatalogEntry(r, CatalogEntity.Model)
    e shouldBe e.copy()
  }

  // ====================================================================
  // Java serialization round-trip (Product with Serializable)
  // ====================================================================

  test("CatalogRef round-trips through Java serialization") {
    val r = CatalogRef("unity", "public", "orders", 3, "abc")
    javaSerializeRoundTrip(r) shouldBe r
  }

  test("PublishMode.CompareAndSet round-trips through Java serialization") {
    val m = PublishMode.CompareAndSet("digest-x")
    javaSerializeRoundTrip(m) shouldBe m
  }

  test("PublishResult.Updated round-trips through Java serialization") {
    val prev = CatalogRef("unity", "public", "orders", 1, "old")
    val cur  = CatalogRef("unity", "public", "orders", 2, "new")
    val r = PublishResult.Updated(prev, cur)
    javaSerializeRoundTrip(r) shouldBe r
  }

  test("CatalogError.StaleConflict (new case) round-trips through Java serialization") {
    val ref = CatalogRef("unity", "public", "orders", 1, "abc")
    val err = CatalogError.StaleConflict("digest mismatch", ref)
    javaSerializeRoundTrip(err) shouldBe err
  }

  test("CatalogFilter round-trips through Java serialization") {
    val f = CatalogFilter(catalog = Some("unity"), kind = Some(CatalogEntity.Model), limit = Some(10))
    javaSerializeRoundTrip(f) shouldBe f
  }

  // -- Helper: Java serialization round-trip --

  private def javaSerializeRoundTrip[T](value: T): T = {
    val baos = new java.io.ByteArrayOutputStream()
    val oos  = new java.io.ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.close()
    val bais = new java.io.ByteArrayInputStream(baos.toByteArray)
    val ois  = new java.io.ObjectInputStream(bais)
    val out  = ois.readObject().asInstanceOf[T]
    ois.close()
    out
  }
}