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
    // Per DE re-review of PR #410 (#5): empty expectedDigest is a
    // degenerate CAS condition with no useful interpretation.
    val ex = intercept[IllegalArgumentException] {
      PublishMode.compareAndSet("")
    }
    ex.getMessage should include ("non-empty")
  }

  test("PublishMode.CompareAndSet rejects null digest (smart constructor)") {
    // Per DE re-review N8: the smart ctor's require clause also
    // rejects null; this test pins that.
    intercept[IllegalArgumentException] {
      PublishMode.compareAndSet(null)
    }
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
    * derived from the doc, per the SWE re-review of PR #410 C1).
    *
    * Per the DE re-review #1 (CRITICAL N1) + SWE re-review HIGH-1:
    * publish and discover MUST share the same key (otherwise the
    * discover exact-match code path is unreachable). Both key on
    * `identity.toString`. `discover` then compares the FULL ref
    * (version + digest) for stale detection: if the stored ref
    * matches the requested ref exactly → Right(Some(doc));
    * if same identity but different version/digest → Right(None)
    * (stale); if no entry → Right(None) (absent).
    *
    * The fake is NOT thread-safe (per the DE review #2):
    * concurrent `publish` calls on the same identity can race.
    * The production adapter implementation is required to be
    * server-side-atomic (per the CatalogAdapter scaladoc). The
    * fake models the serial case for unit tests; concurrent
    * tests are deferred to integration tests with real catalogs. */
  private final class FakeCatalogAdapter extends CatalogAdapter {
    override def catalog: String = "fake"
    private val store = scala.collection.mutable.LinkedHashMap.empty[String, (CatalogRef, Any)]

    override def publish(
        identity: CatalogIdentity,
        doc:     Any,
        as:      CatalogEntity,
        mode:    PublishMode,
    ): Either[CatalogError, PublishResult] = {
      // Per DE re-review #4: CAS uses an explicit target identity
      // (passed in), not derived from the doc.
      val key = identity.toString
      // ManifestDocument is Any (placeholder; real type in PR 6).
      // For the fake, we use a null-safe synthetic digest.
      val newDigest = if (doc == null) "doc-placeholder" else doc.toString
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

    override def discover(ref: CatalogRef): Either[CatalogError, Option[Any]] = {
      // Per DE re-review N1 + SWE re-review HIGH-1: key on the
      // SAME identity (ref.identity) that publish used. Then
      // compare FULL ref for stale detection.
      store.get(ref.identity.toString) match {
        case Some((stored, doc)) if stored == ref =>
          // Exact match. Return Some(doc) so the test can
          // distinguish from stale / absent.
          Right(Some(doc))
        case Some((_, _)) =>
          // Same identity, different version/digest → stale.
          Right(None)
        case None =>
          // No entry at this identity → absent.
          Right(None)
      }
    }

    override def list(
        filter: CatalogFilter,
    ): Either[CatalogError, List[CatalogEntry]] = {
      // Per DE re-review N4: iteration order is now
      // DETERMINISTIC (LinkedHashMap insertion order) and
      // documented as "insertion order" in the trait scaladoc.
      val all = store.values.toList.map(_._1)
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
  // TODO(PR6): when `ManifestDocument = Any` becomes a real ADT,
  // replace every `null` doc arg with a real ManifestDocument value.
  // The fake currently derives digests from `doc.toString` (null-safe
  // to "doc-placeholder"), which will need to change.

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
    // Per DE re-review N1: this test must actually use the
    // STORED ref (not one with a different digest), otherwise
    // the exact-match code path is unreachable. We capture
    // the ref returned by publish and pass it back.
    val adapter = new FakeCatalogAdapter
    val insertResult = adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    val storedRef = insertResult.toOption.get match {
      case PublishResult.Inserted(r) => r
      case other => fail(s"unexpected: $other")
    }
    val result = adapter.discover(storedRef)
    result.toOption.get shouldBe a [Some[_]]
    result.toOption.get.isDefined shouldBe true
  }

  test("discover on stale ref (wrong version): Right(None)") {
    // Per DE review #3: stale refs (wrong version/digest)
    // return None. Same identity as stored, but version 99
    // (stored is version 1) → stale → None.
    val adapter = new FakeCatalogAdapter
    adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    val staleRef = CatalogRef("fake", "public", "orders", 99, "doc-placeholder")
    adapter.discover(staleRef) shouldBe Right(None)
  }

  test("discover on stale ref (wrong digest): Right(None)") {
    // Same identity as stored, but wrong digest → stale → None.
    val adapter = new FakeCatalogAdapter
    adapter.publish(ordersIdentity, null, CatalogEntity.Model, PublishMode.CreateOnly)
    val staleRef = CatalogRef("fake", "public", "orders", 1, "wrong-digest")
    adapter.discover(staleRef) shouldBe Right(None)
  }

  test("discover on absent identity: Right(None) (NOT Left)") {
    // No entry at this identity → absent → None.
    val adapter = new FakeCatalogAdapter
    val ref = CatalogRef("nonexistent", "public", "orders", 1, "abc")
    adapter.discover(ref) shouldBe Right(None)
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

  // Per DE re-review N6: fill the serialization-test coverage gaps.
  test("CatalogIdentity round-trips through Java serialization") {
    val id = CatalogIdentity("unity", "public", "orders")
    javaSerializeRoundTrip(id) shouldBe id
  }

  test("CatalogEntity singletons round-trip through Java serialization") {
    javaSerializeRoundTrip(CatalogEntity.Model) shouldBe CatalogEntity.Model
    javaSerializeRoundTrip(CatalogEntity.Rollup) shouldBe CatalogEntity.Rollup
    javaSerializeRoundTrip(CatalogEntity.ExtensionBlob) shouldBe CatalogEntity.ExtensionBlob
  }

  test("CatalogEntry round-trips through Java serialization") {
    val r = CatalogRef("unity", "public", "orders", 1, "abc")
    val e = CatalogEntry(r, CatalogEntity.Model, Map("owner" -> "analytics"))
    javaSerializeRoundTrip(e) shouldBe e
  }

  test("PublishResult.Inserted round-trips through Java serialization") {
    val r = CatalogRef("unity", "public", "orders", 1, "abc")
    javaSerializeRoundTrip(PublishResult.Inserted(r)) shouldBe PublishResult.Inserted(r)
  }

  test("PublishResult.Conflict (with current) round-trips through Java serialization") {
    val r = CatalogRef("unity", "public", "orders", 1, "abc")
    val c = PublishResult.Conflict("digest mismatch", Some(r))
    javaSerializeRoundTrip(c) shouldBe c
  }

  test("PublishResult.Conflict (no current) round-trips through Java serialization") {
    val c = PublishResult.Conflict("permission denied")
    javaSerializeRoundTrip(c) shouldBe c
  }

  test("CatalogError.Conflict (no current) round-trips through Java serialization") {
    // Per DE re-review N6: the no-arg `Conflict` no longer has a
    // serialization test after the H4 split. Restore it.
    val err = CatalogError.Conflict("reason")
    javaSerializeRoundTrip(err) shouldBe err
  }

  test("CatalogError.Unauthorized round-trips through Java serialization") {
    javaSerializeRoundTrip(CatalogError.Unauthorized("no perm")) shouldBe CatalogError.Unauthorized("no perm")
  }

  test("CatalogError.Network round-trips through Java serialization") {
    javaSerializeRoundTrip(CatalogError.Network("timeout")) shouldBe CatalogError.Network("timeout")
  }

  test("CatalogError.Unsupported round-trips through Java serialization") {
    javaSerializeRoundTrip(CatalogError.Unsupported("read-only")) shouldBe CatalogError.Unsupported("read-only")
  }

  test("CatalogError.MalformedManifest round-trips through Java serialization") {
    javaSerializeRoundTrip(CatalogError.MalformedManifest("missing fields")) shouldBe CatalogError.MalformedManifest("missing fields")
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