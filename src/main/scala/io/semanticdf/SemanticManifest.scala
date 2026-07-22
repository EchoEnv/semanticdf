package io.semanticdf

import java.time.Instant
import java.time.format.DateTimeFormatter

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{ObjectMapper, DeserializationFeature, SerializationFeature}

import org.apache.spark.sql.DataFrame

/** Persist a [[SemanticTable]] to a portable JSON artifact and parse it back.
  *
  * The "manifest" carries the model's *static definition* (identity,
  * dimensions, measures, joins, filters, plus a digest header). It does
  * NOT carry the model's *computed output* — the operator program owns
  * data lifecycle (the streaming terminal's operator-side boundary).
  *
  * Schema: `v0.1.9-manifest`. See `docs/design/manifest-artifact.md` for
  * the full spec. The recipe (§1) said "no new external deps"; we
  * deviated by using `jackson-databind` (already a project dep via
  * `semanticdf-mcp`, version-aligned to Spark 2.15.2) instead of a
  * hand-rolled JSON parser. Trade-off: ~250 LOC of buggy parser/serializer
  * saved vs. one explicit library-pom dependency. */
object SemanticManifest {

  /** Manifest schema version. Bumped only on breaking changes to the
    * JSON shape. */
  val CurrentSchemaVersion: String = "v0.1.11-manifest"

  /** Version-gate accepted by `parseMeta` (prefix match, not strict
    * equality). v0.1.9 / v0.1.10 / v0.1.11 manifests all parse cleanly. */
  private val SupportedSchemaPrefix: String = "v0.1."

  /** The `$schema` URL the manifest points to for tooling validation.
    * Validators fetch this URL to retrieve the JSON Schema document.
    * Tools ignore the value if unreachable; consumers that don't
    * validate don't see the field. */
  val SchemaUrl: String =
    "https://raw.githubusercontent.com/EchoEnv/semanticdf/main/schemas/manifest.schema.json"

  /** Initial `manifestVersion` for v1.0 of the spec. Pre-1.0 to flag
    * the spec as evolving; bumps to 1.0.0 when stable. */
  val InitialManifestVersion: String = "0.1.0"

  /** Sentinel recorded for a dimension/measure whose `exprString` was
    * `None` at serialization time. Original lambda is NOT recoverable. */
  val LambdaSentinel: String = "<lambda>"

  /** Manifest identity-only header. Source-free; usable by tooling that
    * needs to inspect a manifest without loading Spark. */
  final case class ManifestMeta(
      schemaVersion:    String,
      kind:             String,
      // Identity + governance fields (added in v0.1.11, recipe: docs/design/manifest-identity-bump.md).
      // All optional at the schema level; missing → None / empty.
      manifestVersion:  Option[String] = None,    // semver of the spec (e.g. "0.1.0")
      id:               Option[String] = None,    // reverse-DNS FQN (cross-referencing handle)
      namespace:        Option[String] = None,    // multi-env scoping (e.g. "dev", "prod")
      metadata:         Map[String, String] = Map.empty,  // free-form audit object
      // Existing fields:
      modelName:        Option[String],
      version:          Int,
      description:      Option[String],
      sourceTable:      Option[String],
      status:           String,    // "draft" | "published" | "deprecated" — wire format
      dimensions:       Int,
      measures:         Int,
      calcMeasures:     Int,
      joins:            Int,
      filters:          Int,
      isStreaming:      Boolean,
      usesTAll:         Boolean,
  )

  private val mapper: ObjectMapper = new ObjectMapper()
    .registerModule(new com.fasterxml.jackson.module.scala.DefaultScalaModule)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  /** Identity + governance fields for the manifest. The CLI (or any writer)
    * constructs one of these and passes it to `toJson`. The single-arg
    * `toJson(model)` uses `Identity.empty` (omits the new fields) for
    * back-compat with existing tests and callers.
    *
    * `id` is required at the CLI level (recipe §11 Q1); the empty default
    * is for the no-Identity back-compat path only. */
  final case class Identity(
      id:              String,                  // reverse-DNS FQN
      manifestVersion: String = InitialManifestVersion,
      namespace:       String = "default",
      metadata:        Map[String, String] = Map.empty,
  )
  object Identity {
    /** Back-compat default: empty id, no metadata. Existing single-arg
      * `toJson` calls emit a manifest without the new fields. CLI passes
      * an explicit Identity. */
    val empty: Identity = Identity(id = "")
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /** Serialize a [[SemanticTable]] to a JSON manifest string. Pass an
    * explicit `identity` to populate the new identity + governance
    * fields (`id`, `manifestVersion`, `$schema`, `namespace`,
    * `metadata`). Omit for the back-compat single-arg call path —
    * uses `Identity.empty` (no new fields emitted).
    *
    * `identity` and `prettyPrint` both have defaults so callers can use
    * any subset. The CLI uses an explicit `Identity`; existing tests
    * use the single-arg path. */
  def toJson(
      model: SemanticTable,
      identity: Identity = Identity.empty,
      prettyPrint: Boolean = true,
  ): String = {
    if (joinedRoot(model).isDefined)
      throw new IllegalStateException(
        "SemanticManifest.toJson: joined models (SemanticJoinOp root) are not supported. " +
        "See docs/design/manifest-artifact.md §10.")

    val root = buildJsonTree(model, identity)
    val out  = mapper.writeValueAsString(root)
    if (prettyPrint) out else out
  }

  /** Parse a manifest JSON string back into a [[SemanticTable]]. The caller
    * provides a `source` `DataFrame` because every `SemanticTable` is rooted
    * in either a batch table op or a streaming op, both of which hold a
    * `DataFrame` reference. For query purposes `source` should match the
    * manifest's `model.sourceTable`; for meta-only inspection any source works. */
  def fromJson(text: String, source: DataFrame): SemanticTable = {
    val tree = mapper.readTree(text)
    val obj  = requireObject(tree, "manifest root")
    readManifest(obj, source)
  }

  /** Read just the meta header (identity + digest counts) from a manifest
    * JSON. Source-free; intended for `tools.Main validate-manifest` CLI
    * and similar inspection tooling. */
  def parseMeta(text: String): ManifestMeta = {
    val tree = mapper.readTree(text)
    val obj  = requireObject(tree, "manifest root")
    val fields = obj.fieldNames.asScala.toList
    val schemaVersion = optStringField(obj, "schemaVersion")
      .getOrElse(throw ManifestParsingException("missing `schemaVersion`"))
    // Prefix match: v0.1.9 / v0.1.10 / v0.1.11 all parse cleanly. A strict
    // equality check would break every existing v0.1.9 / v0.1.10 manifest
    // when the schemaVersion string bumps. Adding optional fields is
    // non-breaking; the schemaVersion string is bumped but old values
    // still parse.
    if (!schemaVersion.startsWith(SupportedSchemaPrefix))
      throw ManifestParsingException(
        s"manifest schemaVersion is '$schemaVersion', expected prefix '$SupportedSchemaPrefix*'.")
    val kind = optStringField(obj, "kind")
      .getOrElse(throw ManifestParsingException("missing `kind`"))
    if (kind != "semanticdf-model-manifest")
      throw ManifestParsingException(s"unknown manifest kind '$kind'")

    val dig = obj.path("digest")
    val mod = obj.path("model")
    // Identity + governance fields (added in v0.1.11). All optional at
    // the schema level; missing → None / empty.
    val metadataObj = obj.path("metadata")
    val metadataMap: Map[String, String] =
      if (metadataObj.isObject) metadataObj.fieldNames.asScala.toList.map { k =>
        k -> (if (metadataObj.get(k).isTextual) metadataObj.get(k).asText("") else metadataObj.get(k).toString)
      }.toMap
      else Map.empty
    ManifestMeta(
      schemaVersion   = schemaVersion,
      kind            = kind,
      manifestVersion = optStringField(obj, "manifestVersion").orElse(Some(InitialManifestVersion)),
      id              = optStringField(obj, "id"),
      namespace       = optStringField(obj, "namespace").orElse(Some("default")),
      metadata        = metadataMap,
      modelName       = optStringField(mod, "name"),
      version         = optIntField(mod, "version").getOrElse(0),
      description     = optStringField(mod, "description"),
      sourceTable     = optStringField(mod, "sourceTable"),
      status          = optStringField(mod, "status").getOrElse("published"),
      dimensions      = optIntField(dig, "dimensions").getOrElse(0),
      measures        = optIntField(dig, "measures").getOrElse(0),
      calcMeasures    = optIntField(dig, "calcMeasures").getOrElse(0),
      joins           = optIntField(dig, "joins").getOrElse(0),
      filters         = optIntField(dig, "filters").getOrElse(0),
      isStreaming     = optBoolField(dig, "isStreaming"),
      usesTAll        = optBoolField(dig, "usesTAll"),
    )
  }

  /** Derive a per-side `Identity` for the left or right side of a
    * joined model. The joined model's FQN becomes the parent; the
    * side's name is appended with a `.` separator (e.g.
    * `io.acme.warehouse.orders` -> `io.acme.warehouse.orders.customers`).
    *
    * Why this exists: the writer (`toJson`) rejects joined roots
    * (recipe §10 anti-scope), and the `joined-models-manifest` recipe
    * is BLOCKed on `SemanticJoinOp` not carrying enough side metadata.
    * Until a joined-manifest wire shape exists, operators who need a
    * portable record of a joined model emit one single-table manifest
    * per side and hand-compose the joined envelope. This helper does
    * the FQN derivation so callers don't repeat the naming rule.
    *
    * Side `name` is the YAML model's top-level key. For the common
    * "YAML = `<root>` + `joins:[<sideA>]`" pattern, the root YAML key
    * and the joined side's name may collide (e.g. both `orders`); in
    * that case use the explicit `name` parameter to disambiguate.
    *
    * Usage:
    * {{{
    *   val parent = Identity(id = "io.acme.warehouse.orders", namespace = "prod")
    *   val custId = SemanticManifest.sideIdentity(parent, "left",  "customers")
    *   val ordId  = SemanticManifest.sideIdentity(parent, "right", "orders")
    *   val (l, r) = (SemanticManifest.toJson(leftSide,  custId),
    *                 SemanticManifest.toJson(rightSide, ordId))
    * }}}
    *
    * The result keeps the parent's `manifestVersion`, `namespace`, and
    * `metadata` so per-side manifests are aligned with the parent's
    * governance. Drop the result through `Identity.copy(...)` if a
    * caller wants to override per side (e.g. different `namespace`). */
  def sideIdentity(
      parent:   Identity,
      sideLabel: String,
      name:     String,
  ): Identity = parent.copy(
    id       = s"${parent.id}.$name",
    metadata = parent.metadata + ("side" -> sideLabel),
  )

  // ---------------------------------------------------------------------------
  // Tree construction (the part we own — Jackson handles the actual JSON bytes)
  // ---------------------------------------------------------------------------

  private def buildJsonTree(
      model: SemanticTable,
      identity: Identity,
  ): com.fasterxml.jackson.databind.JsonNode = {
    val root = mapper.createObjectNode()

    root.put("schemaVersion", CurrentSchemaVersion)
    root.put("kind",          "semanticdf-model-manifest")
    root.put("compiledAt",    DateTimeFormatter.ISO_INSTANT.format(Instant.now()))

    // Identity + governance fields (recipe: docs/design/manifest-identity-bump.md).
    // Only emitted when the writer passed a non-empty Identity — the single-arg
    // `toJson` path (back-compat for existing tests) passes `Identity.empty`
    // and produces a manifest with no new top-level fields.
    if (identity.id.nonEmpty || identity.metadata.nonEmpty || identity.manifestVersion != InitialManifestVersion || identity.namespace != "default") {
      root.put("manifestVersion", identity.manifestVersion)
      if (identity.id.nonEmpty) root.put("id", identity.id)
      if (identity.namespace.nonEmpty) root.put("namespace", identity.namespace)
      if (identity.metadata.nonEmpty) {
        val metaObj = root.putObject("metadata")
        identity.metadata.foreach { case (k, v) => metaObj.put(k, v) }
      }
      // Always emit $schema last so consumers can validate the shape
      // against a JSON Schema document. The URL is fixed (per the recipe).
      root.put("$schema", SchemaUrl)
    }

    // model
    val modelObj = root.putObject("model")
    putOptString(modelObj, "name",        model.name)
    modelObj.put("version",     model.version)
    modelObj.put("status",      model.status.asString)
    putOptString(modelObj, "description", model.description)
    putOptString(modelObj, "sourceTable", model.sourceTable)

    // digest
    val dims     = model.dimensions.values.toList
    val measures = model.measures.values.toList
    val digObj = root.putObject("digest")
    digObj.put("dimensions",            dims.size)
    digObj.put("timeDimensions",        dims.count(_.isTimeDimension))
    digObj.put("derivedTimeDimensions", dims.count(d => d.isTimeDimension && d.derived.nonEmpty))
    digObj.put("measures",              measures.size)
    digObj.put("calcMeasures",          measures.count(m => model.measureKind(m.name) == MeasureKind.Calc))
    digObj.put("joins",                 0)  // single-table only (§10)
    digObj.put("filters",               model.filters.size)
    digObj.put("isStreaming",           findStreamOp(model.root).isDefined)
    digObj.put("usesTAll",              StreamingSupport.StreamingValidator.findTotalUsers(model.measures.toSeq).nonEmpty)

    // dimensions[]
    val dimsArr = root.putArray("dimensions")
    dims.foreach { d =>
      val obj = dimsArr.addObject()
      obj.put("name",            d.name)
      obj.put("kind",            dimKind(d))
      obj.put("expr",            d.exprString.getOrElse(LambdaSentinel))
      obj.put("isTimeDimension", d.isTimeDimension)
      obj.put("isEntity",        d.isEntity)
      putOptString(obj, "smallestTimeGrain", d.smallestTimeGrain)
      obj.put("isDerived",       d.isTimeDimension && d.derived.nonEmpty)
    }

    // measures[]
    val measArr = root.putArray("measures")
    measures.foreach { m =>
      val kind     = model.measureKind(m.name) match {
        case MeasureKind.Base => "base"
        case MeasureKind.Calc => "calc"
      }
      val expr     = m.exprString.getOrElse(LambdaSentinel)
      val deps     = if (kind == "calc") calcDependencies(m, model) else Seq.empty
      val obj = measArr.addObject()
      obj.put("name", m.name)
      obj.put("kind", kind)
      obj.put("expr", expr)
      val depsArr = obj.putArray("dependsOn")
      deps.foreach(d => depsArr.add(d))
    }

    // joins — empty array (single-table only)
    root.putArray("joins")

    // filters[]
    val filters = model.filters
    val filtArr = root.putArray("filters")
    filters.foreach { f =>
      val obj = filtArr.addObject()
      obj.put("name",      f.name)
      obj.put("expr",      f.expr)
      obj.put("appliedAt", "pre_aggregate")
    }

    root
  }

  // ---------------------------------------------------------------------------
  // Deserialization: tree → SemanticTable
  // ---------------------------------------------------------------------------

  private def readManifest(obj: com.fasterxml.jackson.databind.JsonNode, source: DataFrame): SemanticTable = {
    // Validation — early-fail loud.
    val schemaVersion = optStringField(obj, "schemaVersion").getOrElse(
      throw ManifestParsingException("missing `schemaVersion`"))
    // Prefix match: v0.1.9 / v0.1.10 / v0.1.11 all parse cleanly. See
    // `parseMeta` for the rationale.
    if (!schemaVersion.startsWith(SupportedSchemaPrefix))
      throw ManifestParsingException(
        s"manifest schemaVersion is '$schemaVersion', expected prefix '$SupportedSchemaPrefix*'.")
    val kind = optStringField(obj, "kind").getOrElse(
      throw ManifestParsingException("missing `kind`"))
    if (kind != "semanticdf-model-manifest")
      throw ManifestParsingException(s"unknown manifest kind '$kind'")

    val model = obj.path("model")
    val name        = optStringField(model, "name")
    val description = optStringField(model, "description")
    val sourceTable = optStringField(model, "sourceTable")
    val version     = optIntField(model, "version").getOrElse(0)
    val isStreaming = optBoolField(obj.path("digest"), "isStreaming")

    val dims     = readArr(obj, "dimensions").flatMap(readDimension).toMap
    val measures = readArr(obj, "measures").flatMap(readMeasure).toMap
    val filters  = readArr(obj, "filters").flatMap(readFilter)

    val base: SemanticOp = if (isStreaming) {
      SemanticStreamingTableOp(
        stream      = source,
        name        = name,
        description = description,
        dimensions  = dims,
        measures    = measures,
      )
    } else {
      SemanticTableOp(
        table       = source,
        name        = name,
        description = description,
        dimensions  = dims,
        measures    = measures,
      )
    }

    // Wrap the root in row filters we read from the manifest. foldRight
    // keeps the iteration order so model.filters matches the manifest.
    val withFiltersRoot = filters.foldRight(base) { (f, op) =>
      SemanticRowFilterOp(op, f.name, None, f.expr, Map.empty)
    }

    new SemanticTable(
      root        = withFiltersRoot,
      version     = version,
      sourceTable = sourceTable,
    )
  }

  private def readDimension(node: com.fasterxml.jackson.databind.JsonNode): Option[(String, Dimension)] = {
    val n   = optStringField(node, "name")
    val e   = optStringField(node, "expr")
    if (n.isEmpty || e.isEmpty) return None
    val name            = n.get
    val expr            = e.get
    val isTime          = optBoolField(node, "isTimeDimension")
    val isEntity        = optBoolField(node, "isEntity")
    val smallestGrain   = optStringField(node, "smallestTimeGrain")
    val isSentinel      = expr == LambdaSentinel
    // Lambda-sentinel: produce a placeholder column expression. A consumer
    // who needs the original lambda behavior must re-load from YAML.
    val sentinelCol: org.apache.spark.sql.Column =
      org.apache.spark.sql.functions.col(name)
    val exprString      = if (isSentinel) None else Some(expr)
    val dim: Dimension = {
      val base =
        if (isTime) Dimension.time(name, _ => sentinelCol, smallestTimeGrain = smallestGrain)
        else if (isEntity) Dimension.entity(name, _ => sentinelCol)
        else Dimension(name, _ => sentinelCol)
      if (isSentinel) base else base.copy(exprString = Some(expr))
    }
    Some((name, dim))
  }

  private def readMeasure(node: com.fasterxml.jackson.databind.JsonNode): Option[(String, Measure)] = {
    for {
      n <- optStringField(node, "name")
      e <- optStringField(node, "expr")
    } yield {
      val isSentinel = e == LambdaSentinel
      // When the original lambda was serializable (exprString was set at
      // write time), the persisted `expr` string IS the measure's body —
      // we can rebuild a working measure by parsing the string into a
      // Spark Column. This is the common case for base measures
      // (e.g. `sum(amount)`, `count(1)`) which are typically written by
      // the YAML loader with the `expr:` string in scope.
      //
      // When the original lambda was a bare Scala closure with no hint
      // (exprString was None at write time), the manifest stores
      // LambdaSentinel = "<lambda>" — we cannot reconstruct the
      // behavior, so the measure body is a placeholder `lit(0)` and a
      // loud runtime error fires on first query. The recipe documents
      // this lossiness; consumers needing the original lambda behavior
      // must re-load from YAML.
      val exprString = if (isSentinel) None else Some(e)
      // The `kind` field disambiguates base vs calc measures. Both
      // round-trip via the persisted `expr` string, but the body shape
      // differs:
      //   - **Base measures** reference SOURCE columns (`sum(amount)`,
      //     `count(1)`). Their expr is a normal Spark SQL expression
      //     and can be evaluated via `F.expr` directly.
      //   - **Calc measures** reference OTHER MEASURES
      //     (`total_ship_days / order_count`). The aggregation framework
      //     computes base measures first, then evaluates calc measures
      //     against the post-aggregated DataFrame. To participate in
      //     that, the calc lambda must resolve measure names through
      //     the `SemanticScope` — `CalcExpr.apply(scope, e)` does exactly
      //     that, walking the calc DSL and substituting `scope(name)`
      //     for each ident.
      //
      // Both paths populate `ClassificationScope.referencedMeasures`
      // correctly so the transitive-closure walker pulls in the
      // dependencies, and the post-aggregation `MeasureScope.apply`
      // resolves the column references in the post-agg DataFrame.
      //
      // When the original lambda was a bare Scala closure with no hint
      // (exprString was None at write time), the manifest stores
      // LambdaSentinel = "<lambda>" — we cannot reconstruct the
      // behavior, so the measure body is a placeholder and a loud
      // runtime error fires on first query. The recipe documents this
      // lossiness; consumers needing the original lambda behavior must
      // re-load from YAML.
      val kind = optStringField(node, "kind").getOrElse("base")
      val measureBody: SemanticScope => org.apache.spark.sql.Column =
        if (isSentinel) {
          scope => throw new IllegalStateException(
            s"manifest measure '$n' was a bare lambda at write time (no exprString hint); " +
            s"the original behavior is not recoverable from the manifest. " +
            s"Re-load from YAML to recover. " +
            s"See docs/design/manifest-artifact.md §5.")
        } else if (kind == "calc") {
          // Calc measures reference sibling measures. Use CalcExpr to
          // walk the expr through the scope — every ident becomes
          // `scope(name)`, which the aggregation framework's post-agg
          // MeasureScope resolves to a column.
          scope => CalcExpr(scope, e)
        } else {
          // Base measures reference source columns. Spark's expr parser
          // handles function calls like `sum(amount)` and `count(1)`;
          // the lambda body never needs the scope for these.
          scope => org.apache.spark.sql.functions.expr(e)
        }
      (n, Measure(name = n, expr = measureBody, exprString = exprString))
    }
  }

  private def readFilter(node: com.fasterxml.jackson.databind.JsonNode): Option[SemanticFilter] = {
    for {
      n <- optStringField(node, "name")
      e <- optStringField(node, "expr")
    } yield SemanticFilter(n, None, e, Map.empty)
  }

  // ---------------------------------------------------------------------------
  // Recognition helpers
  // ---------------------------------------------------------------------------

  /** Returns Some(j) if any node in the op-tree wraps a SemanticJoinOp.
    * Walks the same passthrough ops as SemanticTable.toStreamingQuery's
    * findStream so model.groupBy(...).aggregate(...) around a join is
    * also flagged. */
  private def joinedRoot(model: SemanticTable): Option[SemanticOp] = {
    def walk(op: SemanticOp): Option[SemanticOp] = op match {
      case j: SemanticJoinOp        => Some(j)
      case a: SemanticAggregateOp   => walk(a.source)
      case f: SemanticFilterOp      => walk(f.source)
      case rf: SemanticRowFilterOp  => walk(rf.source)
      case o: SemanticOrderByOp     => walk(o.source)
      case l: SemanticLimitOp       => walk(l.source)
      case h: SemanticHintOp        => walk(h.source)
      case tr: SemanticTransformsOp => walk(tr.source)
      case _                       => None
    }
    walk(model.root)
  }

  /** Returns Some(streamOp) if any node in the op-tree wraps a
    * SemanticStreamingTableOp. Mirrors SemanticTable's findStream walk
    * (recipe §1). */
  private def findStreamOp(op: SemanticOp): Option[SemanticStreamingTableOp] = {
    def walk(o: SemanticOp): Option[SemanticStreamingTableOp] = o match {
      case s: SemanticStreamingTableOp => Some(s)
      case a: SemanticAggregateOp      => walk(a.source)
      case f: SemanticFilterOp         => walk(f.source)
      case rf: SemanticRowFilterOp     => walk(rf.source)
      case o: SemanticOrderByOp        => walk(o.source)
      case l: SemanticLimitOp          => walk(l.source)
      case h: SemanticHintOp           => walk(h.source)
      case tr: SemanticTransformsOp    => walk(tr.source)
      case _                          => None
    }
    walk(op)
  }

  /** Dim kind classifier. Order: derived-time wins over plain time wins
    * over entity wins over plain categorical. */
  private def dimKind(d: Dimension): String = {
    if (d.isTimeDimension && d.derived.nonEmpty) "derived-time"
    else if (d.isTimeDimension) "time"
    else if (d.isEntity) "entity"
    else "categorical"
  }

  /** Measure names a calc measure references. Returns Seq.empty on any
    * probe failure. */
  private def calcDependencies(m: Measure, model: SemanticTable): Seq[String] = {
    val known  = model.measures.keySet
    val source = rootDataFrame(model)
    val probe  = new ClassificationScope(source, known)
    try { m.expr(probe) } catch { case _: Throwable => return Seq.empty }
    probe.referencedMeasures.toList.sorted
  }

  /** Walk the op-tree to find the root-level DataFrame (the source the
    * model was built against). For a single-table model this is either
    * the table- or stream-op's DataFrame. */
  private def rootDataFrame(model: SemanticTable): DataFrame = {
    def walk(op: SemanticOp): DataFrame = op match {
      case s: SemanticTableOp         => s.table
      case s: SemanticStreamingTableOp => s.stream
      case a: SemanticAggregateOp      => walk(a.source)
      case f: SemanticFilterOp         => walk(f.source)
      case rf: SemanticRowFilterOp     => walk(rf.source)
      case o: SemanticOrderByOp        => walk(o.source)
      case l: SemanticLimitOp          => walk(l.source)
      case h: SemanticHintOp           => walk(h.source)
      case tr: SemanticTransformsOp    => walk(tr.source)
      case _                          =>
        throw new IllegalStateException(
          "SemanticManifest: cannot find root DataFrame in op-tree")
    }
    walk(model.root)
  }

  // ---------------------------------------------------------------------------
  // Jackson field helpers
  // ---------------------------------------------------------------------------

  private def requireObject(tree: com.fasterxml.jackson.databind.JsonNode, ctx: String)
      : com.fasterxml.jackson.databind.JsonNode = tree match {
    case n if n.isObject => n
    case _ => throw ManifestParsingException(s"$ctx must be a JSON object")
  }

  private def optStringField(parent: com.fasterxml.jackson.databind.JsonNode, name: String): Option[String] = {
    Option(parent.get(name)).filter(!_.isNull).map(_.asText())
  }

  /** Render an optional string into a Jackson object — keeps `null` as
    * `null` (Jackson distinguishes), or stores the string. */
  private def putOptString(obj: com.fasterxml.jackson.databind.node.ObjectNode, name: String, value: Option[String]): Unit =
    value match {
      case Some(v) => obj.put(name, v)
      case None    => obj.putNull(name)
    }

  private def optIntField(parent: com.fasterxml.jackson.databind.JsonNode, name: String): Option[Int] = {
    Option(parent.get(name)).filter(!_.isNull).flatMap { n =>
      if (n.isInt) Some(n.asInt()) else if (n.isLong) Some(n.asLong().toInt) else if (n.isNumber) Some(n.asDouble().toInt) else None
    }
  }

  private def optBoolField(parent: com.fasterxml.jackson.databind.JsonNode, name: String): Boolean = {
    Option(parent.get(name)).filter(!_.isNull).exists(_.asBoolean())
  }

  private def readArr(parent: com.fasterxml.jackson.databind.JsonNode, name: String): List[com.fasterxml.jackson.databind.JsonNode] = {
    Option(parent.get(name)).filter(_.isArray) match {
      case Some(arr) =>
        val it = arr.elements()
        val b  = scala.collection.mutable.ListBuffer.empty[com.fasterxml.jackson.databind.JsonNode]
        while (it.hasNext) b += it.next()
        b.toList
      case None => Nil
    }
  }
}

/** Thrown when [[SemanticManifest.fromJson]] or [[SemanticManifest.parseMeta]]
  * fails. */
final case class ManifestParsingException(msg: String)
  extends RuntimeException(s"SemanticManifest: $msg")
