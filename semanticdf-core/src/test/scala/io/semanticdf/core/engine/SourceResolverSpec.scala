package io.semanticdf.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.model.SourceRef

/** A serializable SourceResolver implementation used by
  * SourceResolverSpec to test round-trip serialization. Top-level
  * (not nested) so it doesn't capture the enclosing test class. */
case class FixedResolver(result: ResolvedSource) extends SourceResolver {
  override def resolve(source: SourceRef, identity: EngineIdentity): ResolvedSource = result
}

/** Phase 2 contract: prove `SourceResolver` is a usable, Spark-free
  * contract. Per scala-data-driven-refactor, this is pure contract:
  * the SHAPE is in core; the BODY (engine-specific resolution) is
  * in each engine adapter's implementation.
  */
class SourceResolverSpec extends AnyFunSuite with Matchers {

  private val testIdentity = EngineIdentity(
    name                 = "test",
    nativeVersion        = "0.1.0",
    engineAdapterVersion = "0.3.0",
  )

  private val sampleSource = SourceRef.ByName(
    catalog   = None,
    namespace = Some("public"),
    table     = "orders",
  )

  // -- SourceResolver.noOp singleton --

  test("SourceResolver.noOp is a SourceResolver") {
    SourceResolver.noOp shouldBe a [SourceResolver]
  }

  test("SourceResolver.noOp returns Incompatible for any source") {
    val r = SourceResolver.noOp.resolve(sampleSource, testIdentity)
    r shouldBe a [ResolvedSource.Incompatible]
    r match {
      case ResolvedSource.Incompatible(_, reason) =>
        reason should include ("no resolver")
      case _ => fail("expected Incompatible")
    }
  }

  // -- mock implementation --

  /** A mock resolver that returns a fixed ResolvedSource.Scan
    * for any source — used to test the contract shape. */
  private val scanResolver = new SourceResolver {
    override def resolve(source: SourceRef, identity: EngineIdentity): ResolvedSource =
      ResolvedSource.Scan(
        source = source,
        schema = ResolvedSchema(fields = Map("id" -> "bigint", "name" -> "varchar")),
      )
  }

  test("mock resolver returns ResolvedSource.Scan") {
    val r = scanResolver.resolve(sampleSource, testIdentity)
    r shouldBe a [ResolvedSource.Scan]
  }

  test("mock resolver carries the source through") {
    val r = scanResolver.resolve(sampleSource, testIdentity).asInstanceOf[ResolvedSource.Scan]
    r.source shouldBe sampleSource
  }

  test("mock resolver with ByPath source") {
    val byPath = SourceRef.ByPath(format = "parquet", path = "/data/orders", options = Map.empty)
    val r = scanResolver.resolve(byPath, testIdentity)
    r shouldBe a [ResolvedSource.Scan]
    r.asInstanceOf[ResolvedSource.Scan].source shouldBe byPath
  }

  /** A mock resolver that returns NotFound — for testing the
    * NotFound path. */
  private val notFoundResolver = new SourceResolver {
    override def resolve(source: SourceRef, identity: EngineIdentity): ResolvedSource =
      ResolvedSource.NotFound(source = sampleSource, reason = "table not found")
  }

  test("mock resolver returning NotFound") {
    val r = notFoundResolver.resolve(sampleSource, testIdentity)
    r shouldBe a [ResolvedSource.NotFound]
  }

  /** A mock resolver that returns AuthFailed — for testing the
    * auth-failed path. */
  private val authFailedResolver = new SourceResolver {
    override def resolve(source: SourceRef, identity: EngineIdentity): ResolvedSource =
      ResolvedSource.AuthFailed(source, "kerberos ticket expired")
  }

  test("mock resolver returning AuthFailed") {
    val r = authFailedResolver.resolve(sampleSource, testIdentity)
    r shouldBe a [ResolvedSource.AuthFailed]
    r match {
      case ResolvedSource.AuthFailed(_, reason) =>
        reason should include ("kerberos")
      case _ => fail("expected AuthFailed")
    }
  }

  // -- Serializable round-trip --

  test("SourceResolver implementation round-trips through Java serialization (using a serializable mock)") {
    // FixedResolver is defined as a top-level case class so it
    // doesn't capture the enclosing test instance.
    val original = FixedResolver(ResolvedSource.NotFound(
      source = sampleSource, reason = "table not found",
    ))
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(original)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[FixedResolver]
    restored shouldBe original
  }
}