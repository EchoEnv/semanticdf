package io.semanticdf

import java.time.Instant
import java.time.format.DateTimeFormatter

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{ObjectMapper, DeserializationFeature, SerializationFeature}

import org.apache.spark.sql.{Column, DataFrame}

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

  /** Joined-manifest identity-only header (kind = `semanticdf-joined-manifest`).
    *
    * Source-free; usable by tooling that needs to inspect a joined manifest
    * without loading Spark or providing two source DataFrames. Complements
    * [[ManifestMeta]] for the joined case — the per-side details (left/right
    * dims/measures) are NOT included here because tooling should call
    * [[fromJoinedJson]] for those, or read the raw JSON.
    *
    * The `kind` discriminator matches the writer ([[toJoinedJson]]) so
    * callers can route: `kind == "semanticdf-joined-manifest"` →
    * [[fromJoinedJson]]; otherwise → [[fromJson]].
    *
    * See `docs/design/joined-models-manifest.md` §5 for the field
    * rationale. */
  final case class JoinedManifestMeta(
      schemaVersion:    String,
      kind:             String,
      modelName:        Option[String],
      version:          Int,
      description:      Option[String],
      // Identity + governance fields (mirrors ManifestMeta).
      manifestVersion:  Option[String] = None,
      id:               Option[String] = None,
      namespace:        Option[String] = None,
      metadata:         Map[String, String] = Map.empty,
      // Joined-specific shape from recipe §3.
      cardinality:      String,         // "one" | "many" | "cross"
      leftKeys:         Seq[String],    // from PR #153 key fields; empty for cross joins
      rightKeys:        Seq[String],    // or when the wire shape came from a pre-#153 producer
      multiColumn:      Boolean,        // true if either key array has length > 1
      onExprString:     Option[String], // SQL-form capture from PR #153; non-equi fallback
      // Path C: prefix fields (recipe §3.6, caveat §1.3). Optional;
      // empty string when not set (= canonical post-v0.1.11 producer case).
      leftPrefix:       String = "",
      rightPrefix:      String = "",
      // Path C: extra dims/measures from the joined runtime
      // (recipe §1.2 caveat). Mirrors the writer's extra_dimensions[] /
      // extra_measures[] blocks. Defaults to zero for legacy manifests.
      extraDimensions:  Int = 0,
      extraMeasures:    Int = 0,
      leftDimensions:   Int,
      rightDimensions:  Int,
      mergedDimensions: Int,
      leftMeasures:     Int,
      rightMeasures:    Int,
      mergedMeasures:   Int,
      isStreaming:      Boolean,
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
    if (kind != "semanticdf-model-manifest" && kind != "semanticdf-joined-manifest")
      throw ManifestParsingException(
        s"unknown manifest kind '$kind'. Expected `semanticdf-model-manifest` or `semanticdf-joined-manifest`.")

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

  /** Source-free joined-manifest header for `kind = semanticdf-joined-manifest`.
    *
    * Mirrors [[parseMeta]] for the joined case. Used by `tools.Main
    * validate-joined-manifest` (added in PR #151). Throws
    * [[ManifestParsingException]] if the JSON is not a joined manifest.
    *
    * Implementation note: the joined wire shape per
    * `docs/design/joined-models-manifest.md` §3 is:
    * {{{
    *   {
    *     "schemaVersion": ..., "kind": "semanticdf-joined-manifest",
    *     "model": { "name", "version", "description", "left", "right",
    *                "join": { "cardinality", "leftKeys", "rightKeys" } },
    *     "digest": { "leftDimensions", "rightDimensions", "mergedDimensions",
    *                 "leftMeasures", "rightMeasures", "mergedMeasures",
    *                 "isStreaming" }
    *   }
    * }}}
    *
    * Note: the recipe envisioned per-side manifests inlined under
    * `model.left` / `model.right`. Today, the implementation emits them
    * directly (not as embedded sub-manifests); the BLOCK review
    * identified this as one of the unresolved items. For now
    * `left` / `right` are stub top-level field references (paths to the
    * per-side manifests), kept opaque by the parser.
    */
  def parseJoinedMeta(text: String): JoinedManifestMeta = {
    val tree = mapper.readTree(text)
    val obj  = requireObject(tree, "manifest root")
    val schemaVersion = optStringField(obj, "schemaVersion")
      .getOrElse(throw ManifestParsingException("missing `schemaVersion`"))
    if (!schemaVersion.startsWith(SupportedSchemaPrefix))
      throw ManifestParsingException(
        s"manifest schemaVersion is '$schemaVersion', expected prefix '$SupportedSchemaPrefix*'.")
    val kind = optStringField(obj, "kind")
      .getOrElse(throw ManifestParsingException("missing `kind`"))
    if (kind != "semanticdf-joined-manifest")
      throw ManifestParsingException(
        s"parseJoinedMeta: expected `semanticdf-joined-manifest` kind, got '$kind'")

    val mod = obj.path("model")
    val dig = obj.path("digest")
    val jn  = mod.path("join")
    val metadataObj = obj.path("metadata")
    val metadataMap: Map[String, String] =
      if (metadataObj.isObject) metadataObj.fieldNames.asScala.toList.map { k =>
        k -> (if (metadataObj.get(k).isTextual) metadataObj.get(k).asText("") else metadataObj.get(k).toString)
      }.toMap
      else Map.empty

    JoinedManifestMeta(
      schemaVersion    = schemaVersion,
      kind             = kind,
      modelName        = optStringField(mod, "name"),
      version          = optIntField(mod, "version").getOrElse(0),
      description      = optStringField(mod, "description"),
      manifestVersion  = optStringField(obj, "manifestVersion").orElse(Some(InitialManifestVersion)),
      id               = optStringField(obj, "id"),
      namespace        = optStringField(obj, "namespace").orElse(Some("default")),
      metadata         = metadataMap,
      cardinality      = optStringField(jn, "cardinality").getOrElse("one"),
      leftKeys         = readStringArray(jn.path("leftKeys")),
      rightKeys        = readStringArray(jn.path("rightKeys")),
      // multiColumn is set when either key array carries > 1 element,
      // or explicitly by the writer. Default `false` for legacy
      // / hand-rolled manifests — fall back to length-based derivation.
      multiColumn      =
        if (jn.has("multiColumn")) optBoolField(jn, "multiColumn")
        else readStringArray(jn.path("leftKeys")).length > 1,
      // onExprString is the SQL-form capture for predicates that
      // couldn't factor into the keys arrays (OR / non-equi / mixed).
      // `None` for cross joins or when the writer had no probe at
      // construction.
      onExprString     = optStringField(jn, "onExprString"),
      leftDimensions   = optIntField(dig, "leftDimensions").getOrElse(0),
      rightDimensions  = optIntField(dig, "rightDimensions").getOrElse(0),
      mergedDimensions = optIntField(dig, "mergedDimensions").getOrElse(0),
      leftMeasures     = optIntField(dig, "leftMeasures").getOrElse(0),
      rightMeasures    = optIntField(dig, "rightMeasures").getOrElse(0),
      mergedMeasures   = optIntField(dig, "mergedMeasures").getOrElse(0),
      isStreaming      = optBoolField(dig, "isStreaming"),
      // Path C caveat §1.3: optional prefix fields.
      leftPrefix       = optStringField(jn, "leftPrefix").getOrElse(""),
      rightPrefix      = optStringField(jn, "rightPrefix").getOrElse(""),
      // Path C caveat §1.2: optional extra dims/measures counts.
      // Derived from the actual `model.extra_dimensions[]` /
      // `model.extra_measures[]` arrays in the wire shape (not from
      // the digest). Falls back to 0 for legacy manifests.
      extraDimensions  = readStringArray(mod.path("extra_dimensions")).length,
      extraMeasures    = readStringArray(mod.path("extra_measures")).length,
    )
  }

  /** Read a JSON array field as a `Seq[String]`. Empty for missing
    * or non-array nodes. */
  private def readStringArray(node: com.fasterxml.jackson.databind.JsonNode): Seq[String] =
    if (node.isArray)
      node.elements.asScala.toList.map(_.asText(""))
    else
      Seq.empty

  /** Walk past passthrough ops (filters, transforms) to reach the
    * underlying `SemanticTableOp` (or its streaming equivalent). Used
    * by `fromJoinedJson` so a per-side embedded manifest carrying
    * pre-aggregate filters (e.g. `require_name_not_null`) is reduced
    * to its root when the joined op needs an `Op`. */
  private def unwrapToTableOp(op: SemanticOp): SemanticTableOp = op match {
    case t: SemanticTableOp         => t
    case s: SemanticStreamingTableOp =>
      SemanticTableOp(s.stream, s.name, s.description, s.dimensions, s.measures)
    case f: SemanticRowFilterOp     => unwrapToTableOp(f.source)
    case tr: SemanticTransformsOp   => unwrapToTableOp(tr.source)
    case other =>
      throw new IllegalStateException(
        s"fromJoinedJson: expected a single-table op after unwrapping filters/transforms, " +
        s"got ${other.getClass.getSimpleName}. The per-side embedded manifest must be " +
        s"a single-table manifest (filters/transforms are preserved on the side's " +
        s"SemanticTable but the joined op needs an unwrappable root).")
  }

  /** Emit a joined manifest. Requires the model to be `SemanticJoinOp`-rooted
    * and have both `leftSide` and `rightSide` populated (the foundation
    * `feat/joined-manifest-foundation` PR ensures these are populated for
    * models built via the public `join_*` API).
    *
    * Throws `IllegalStateException` if the model isn't joined-rooted or
    * doesn't carry side metadata. The thrown message names the dispatch
    * rule.
    *
    * See [[toJson]] for the single-table path; this method is the
    * joined counterpart. */
  def toJoinedJson(
      model: SemanticTable,
      identity: Identity = Identity.empty,
      prettyPrint: Boolean = true,
  ): String = {
    val op = model.root match {
      case j: SemanticJoinOp => j
      case other =>
        throw new IllegalStateException(
          s"SemanticManifest.toJoinedJson: expected a SemanticJoinOp-rooted model, " +
          s"got ${other.getClass.getSimpleName}. Use this method only on joined models.")
    }
    val leftT  = op.leftSide.getOrElse(
      throw new IllegalStateException(
        "SemanticManifest.toJoinedJson: joined op has no `leftSide`. " +
        "Construct joins via `SemanticTable.join_*` (which populates the side fields), " +
        "not by hand-assembling SemanticJoinOp, so the originating SemanticTable " +
        "is recoverable. See PR #150 (feat/model: SemanticJoinOp carries originating " +
        "SemanticTable sides)."))
    val rightT = op.rightSide.getOrElse(
      throw new IllegalStateException(
        "SemanticManifest.toJoinedJson: joined op has no `rightSide`. " +
        "See `leftSide` documentation in this method."))

    // Per-side embedded manifests (single-table kind), driven by the
    // identity-bump API. The side identity derives from the parent's
    // identity via sideIdentity.
    val leftId  = sideIdentity(identity, "left",  op.leftRoot.name.getOrElse("left"))
    val rightId = sideIdentity(identity, "right", op.rightRoot.name.getOrElse("right"))
    val leftJson  = toJson(leftT,  leftId,  prettyPrint = prettyPrint)
    val rightJson = toJson(rightT, rightId, prettyPrint = prettyPrint)

    // Compose the joined envelope.
    val merged  = op.mergedModel
    val dimCount = merged.dimensions.size
    val mesCount = merged.measures.size
    val cardinality = op.cardinality match {
      case JoinCardinality.One   => "one"
      case JoinCardinality.Many  => "many"
      case JoinCardinality.Cross => "cross"
    }

    val root = mapper.createObjectNode()
    root.put("schemaVersion", CurrentSchemaVersion)
    root.put("kind",          "semanticdf-joined-manifest")
    root.put("compiledAt",    DateTimeFormatter.ISO_INSTANT.format(Instant.now()))

    if (identity.id.nonEmpty || identity.metadata.nonEmpty || identity.manifestVersion != InitialManifestVersion || identity.namespace != "default") {
      root.put("manifestVersion", identity.manifestVersion)
      if (identity.id.nonEmpty) root.put("id", identity.id)
      if (identity.namespace.nonEmpty) root.put("namespace", identity.namespace)
      if (identity.metadata.nonEmpty) {
        val metaObj = root.putObject("metadata")
        identity.metadata.foreach { case (k, v) => metaObj.put(k, v) }
      }
      root.put("$schema", SchemaUrl)
    }

    val modelObj = root.putObject("model")
    putOptString(modelObj, "name",        merged.name)
    modelObj.put("version",     model.version)
    modelObj.put("status",      model.status.asString)
    putOptString(modelObj, "description", merged.description.orElse(model.description))

    // Embed the per-side manifests as JSON sub-trees. Parse the strings
    // back so we can attach them as nested nodes (Jackson doesn't have a
    // builder for "merge a parsed JSON string at this path").
    val leftNode  = mapper.readTree(leftJson)
    val rightNode = mapper.readTree(rightJson)
    val modelMap  = root.get("model").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
    val placeholder = mapper.createObjectNode()
    modelMap.set[com.fasterxml.jackson.databind.JsonNode]("left",  placeholder)
    modelMap.set[com.fasterxml.jackson.databind.JsonNode]("right", placeholder)
    // Replace the placeholder with the parsed side manifests.
    modelMap.set[com.fasterxml.jackson.databind.JsonNode]("left",  leftNode)
    modelMap.set[com.fasterxml.jackson.databind.JsonNode]("right", rightNode)

    val joinObj = modelObj.putObject("join")
    joinObj.put("cardinality", cardinality)
    // Path C: optional join-side prefix fields (recipe §3.6, caveat §1.3).
    // When set, the consumer is expected to qualify the right/left side
    // columns accordingly. The reconstructed `on` lambda at restore time
    // applies them so the predicate reads
    // `l("<leftPrefix>k1") === r("<rightPrefix>k1")` when set. Omitted when
    // both are empty (the canonical post-v0.1.11 producer case).
    if (op.leftPrefix.nonEmpty)  joinObj.put("leftPrefix",  op.leftPrefix)
    if (op.rightPrefix.nonEmpty) joinObj.put("rightPrefix", op.rightPrefix)
    // leftKeys / rightKeys are populated from `SemanticJoinOp.leftKeys`
    // / `rightKeys` (added in PR #153). They carry the equi-join key
    // column names whether the user typed them via `join_on(...)` /
    // `join_many_on(...)` or whether the lambda-decomposition probe at
    // construction recovered them.
    //
    // For non-equi joins (the probe couldn't factor them into pairs),
    // `op.leftKeys` may be empty. In that case the SQL fallback in
    // `op.onExprString` is the only carrier (see `JoinedManifestMeta`
    // surface below). The kind discriminator (`semanticdf-joined-manifest`)
    // tells consumers which path to take.
    val writeKeyArr = (name: String, keys: Seq[String]) => {
      val arr = joinObj.putArray(name)
      keys.foreach(arr.add)
    }
    writeKeyArr("leftKeys",  op.leftKeys)
    writeKeyArr("rightKeys", op.rightKeys)
    // `onExprString` is the SQL-form capture carried from PR #153 for
    // non-equi predicates or multi-key equis that didn't factor.
    // Emitted on the join block (alongside `cardinality`) so the
    // reader can reach it without searching the model. Empty strings
    // are skipped (the field is optional).
    op.onExprString.foreach { sql => joinObj.put("onExprString", sql) }

    // multiColumn = true when the keys are populated OR when the
    // predicate was reduced to a multi-key equi (length > 1). When
    // both `leftKeys` and `onExprString` are absent (lambda couldn't
    // be factored at all), the read path falls back to a notional
    // single-column model — consumers that need the actual semantics
    // should re-load from YAML.
    joinObj.put("multiColumn", op.leftKeys.length > 1 || op.rightKeys.length > 1)

    // Path C: emit `extra_dimensions` and `extra_measures` blocks for
    // the alias-prefixed dimensions and measures that YamlLoader adds at
    // runtime (e.g. `carriers.name` from a `joins: [customers]`
    // aliasing). These live on `SemanticJoinOp.extraDimensions` /
    // `extraMeasures` and are otherwise dropped on the wire. Skipped
    // when empty (legacy manifests don't carry the field).
    if (op.extraDimensions.nonEmpty) {
      val arr = modelObj.putArray("extra_dimensions")
      op.extraDimensions.values.foreach { d =>
        val obj = arr.addObject()
        obj.put("name", d.name)
        obj.put("kind", dimKind(d))
        obj.put("expr", d.exprString.getOrElse(LambdaSentinel))
        putOptString(obj, "description", d.description)
      }
    }
    if (op.extraMeasures.nonEmpty) {
      val arr = modelObj.putArray("extra_measures")
      op.extraMeasures.values.foreach { m =>
        val obj = arr.addObject()
        obj.put("name", m.name)
        obj.put("kind", "base")
        obj.put("expr", m.exprString.getOrElse(LambdaSentinel))
        putOptString(obj, "description", m.description)
      }
    }

    // Digest (left/right/merged counts).
    val dig = root.putObject("digest")
    val leftDims  = op.leftRoot.dimensions
    val rightDims = op.rightRoot.dimensions
    dig.put("leftDimensions",   leftDims.size)
    dig.put("rightDimensions",  rightDims.size)
    dig.put("mergedDimensions", dimCount)
    dig.put("leftMeasures",     op.leftRoot.measures.size)
    dig.put("rightMeasures",    op.rightRoot.measures.size)
    dig.put("mergedMeasures",   mesCount)
    dig.put("joins",            1)
    dig.put("isStreaming",      findStreamOp(model.root).isDefined)

    val out = mapper.writeValueAsString(root)
    if (prettyPrint) out else out
  }

  /** Reconstruct a `SemanticTable` rooted at `SemanticJoinOp` from a
    * joined manifest JSON. The two source DataFrames are bound by the
    * caller (typically the operator), since the manifest itself doesn't
    * carry data — just metadata.
    *
    * The shape of the input JSON is the joined envelope produced by
    * [[toJoinedJson]]: `kind = "semanticdf-joined-manifest"`, with
    * `model.left` / `model.right` as embedded per-side single-table
    * manifests. Each embedded manifest is fed back through [[readManifest]]
    * with the supplied source DF.
    *
    * Limitations / BLOCK findings surfaced here:
    *   - The `on` lambda cannot be reconstructed from the joined wire
    *     shape without keys. This implementation uses an identity
    *     predicate `l(<no-key>) === r(<no-key>)` that always throws at
    *     evaluation time. To actually evaluate the restored model, the
    *     caller should re-load from YAML (which carries the original
    *     lambda or the join key strings). The metadata round-trip
    *     (name, version, status, dimensions, measures) DOES work.
    *   - The merged-model state carries through (extra dims/measures,
    *     cardinality) — the original complex reconstruction from the
    *     BLOCK recipe §3's "right ++ left ++ extras" pattern is
    *     approximated; full reconstruction requires the keys. */
  def fromJoinedJson(
      text: String,
      leftSource: DataFrame,
      rightSource: DataFrame,
  ): SemanticTable = {
    val tree = mapper.readTree(text)
    val obj  = requireObject(tree, "manifest root")
    val schemaVersion = optStringField(obj, "schemaVersion").getOrElse(
      throw ManifestParsingException("missing `schemaVersion`"))
    if (!schemaVersion.startsWith(SupportedSchemaPrefix))
      throw ManifestParsingException(
        s"manifest schemaVersion is '$schemaVersion', expected prefix '$SupportedSchemaPrefix*'.")
    val kind = optStringField(obj, "kind").getOrElse(
      throw ManifestParsingException("missing `kind`"))
    if (kind != "semanticdf-joined-manifest")
      throw ManifestParsingException(
        s"fromJoinedJson: expected `semanticdf-joined-manifest` kind, got '$kind'")

    val modelObj    = obj.path("model")
    val description = optStringField(modelObj, "description")
    val version     = optIntField(modelObj, "version").getOrElse(0)
    val joinedName  = optStringField(modelObj, "name")
    val joinedStatus= optStringField(modelObj, "status").getOrElse("published")

    // Embedded per-side single-table manifests.
    val leftManifest  = modelObj.path("left")
    val rightManifest = modelObj.path("right")
    val leftT  = if (leftManifest.isObject)  readManifest(leftManifest,  leftSource)  else null
    val rightT = if (rightManifest.isObject) readManifest(rightManifest, rightSource) else null
    if (leftT  == null) throw ManifestParsingException("joined manifest missing `model.left`")
    if (rightT == null) throw ManifestParsingException("joined manifest missing `model.right`")

    val joinObj     = modelObj.path("join")
    val cardinality = joinObj.path("cardinality").asText("one") match {
      case "cross" => JoinCardinality.Cross
      case "many"  => JoinCardinality.Many
      case _       => JoinCardinality.One
    }

    // Reconstruct the `on` lambda from the wire. PR #153 added explicit
    // `leftKeys` / `rightKeys` / `onExprString` fields on
    // `SemanticJoinOp`. The fallback lattice:
    //
    //   - If keys are present (single OR multi-column): rebuild `on`
    //     as `l(k1) === r(k1) AND l(k2) === r(k2) AND ...` against the
    //     per-side `BaseScope`s. This is functional.
    //   - Else if `onExprString` is present (multi-key equi that
    //     didn't factor into parallel arrays, or any non-equi predicate):
    //     reconstruct `on` as `expr(sql)`. Functional for SQL
    //     expressions but loses resolution against the renamed scope
    //     columns. Working fallback.
    //   - Else (legacy wire shape from before this PR): non-functional
    //     throw, BLOCK reference. Modern producers always emit keys OR
    //     SQL, so this branch fires only on hand-rolled wire shapes.
    val jLeftKeys   = readStringArray(joinObj.path("leftKeys"))
    val jRightKeys  = readStringArray(joinObj.path("rightKeys"))
    val jMulti      = optBoolField(joinObj, "multiColumn")
    // Path C: prefix fields (recipe §3.6, caveat §1.3). Optional;
    // default empty (= no prefix, canonical post-v0.1.11 producer case).
    val jLeftPrefix  = optStringField(joinObj, "leftPrefix").getOrElse("")
    val jRightPrefix = optStringField(joinObj, "rightPrefix").getOrElse("")
    val on: (JoinSide, JoinSide) => Column =
      if (jLeftKeys.length == jRightKeys.length && jLeftKeys.nonEmpty) {
        // Path 1: keys present, multi-column or single. AND over pairs.
        // Apply the per-side prefix when reconstructing the predicate
        // (Path C caveat §1.3): the predicate becomes
        //   l("<leftPrefix>k1") === r("<rightPrefix>k1")
        // so joined DataFrames with overlapping column names resolve
        // correctly. Empty prefix = bare column name (default).
        if (jLeftKeys.length == 1) {
          val l = jLeftKeys.head
          val r = jRightKeys.head
          val lP = jLeftPrefix
          val rP = jRightPrefix
          (left: JoinSide, right: JoinSide) => left(s"$lP$l") === right(s"$rP$r")
        } else {
          val lP = jLeftPrefix
          val rP = jRightPrefix
          (left: JoinSide, right: JoinSide) =>
            jLeftKeys.zip(jRightKeys)
              .map { case (lk, rk) => left(s"$lP$lk") === right(s"$rP$rk") }
              .reduce(_ && _)
        }
      } else {
        // Try Path 2: onExprString fallback. The wire carries the
        // SQL form on `digest` adjacent to the join block.
        val sqlOpt = optStringField(modelObj, "onExprString")
        sqlOpt match {
          case Some(sql) =>
            (left: JoinSide, right: JoinSide) => org.apache.spark.sql.functions.expr(sql)
          case None =>
            // Path 3: legacy wire shape. Keep the BLOCK throw so callers
            // get a clear pointer at what's missing — not a silent no-op.
            (left: JoinSide, right: JoinSide) =>
              throw new IllegalStateException(
                "SemanticManifest.fromJoinedJson: this joined manifest has no recoverable " +
                "join key (missing leftKeys[]/rightKeys[] and onExprString). " +
                "Re-load from YAML to get a functional SemanticJoinOp. " +
                "(See docs/design/joined-models-manifest.md §1.)")
        }
      }

    // Multi-column flag is consumed via the helper below; kept as a
    // local for clarity at the use site.
    val _ignored = jMulti

    val leftRoot  = unwrapToTableOp(leftT.root)
    val rightRoot = unwrapToTableOp(rightT.root)

    // Read Path C: extra_dimensions[] and extra_measures[] from the
    // wire shape (Path C closes the alias-prefixed dim caveat §1.2).
    // These are the alias-prefixed dimensions/measures that YamlLoader
    // adds at runtime (e.g. `carriers.name` from a `joins: [customers]`
    // aliasing). We attach them via a `SemanticTransformsOp` wrapper
    // around the base join, matching the runtime's exact wiring.
    val jExtraDims = readArr(modelObj, "extra_dimensions").flatMap(readDimension).toMap
    val jExtraMeasures = readArr(modelObj, "extra_measures").flatMap(readMeasure).toMap

    val baseJoin = SemanticJoinOp(
      left   = leftRoot,
      right  = rightRoot,
      on     = on,
      cardinality = cardinality,
      leftRoot    = leftRoot,
      rightRoot   = rightRoot,
      leftSide    = Some(leftT),
      rightSide   = Some(rightT),
      // Wire-shape carry-over: the keys + onExprString from the writer
      // (PR #153 foundation) preserve the through-trip integrity. If
      // the manifest was hand-rolled or emitted by a pre-#153 writer,
      // these default to empty / None and the join degrades gracefully
      // (the `on` rebuild above still works for the legacy empty case
      // via the throw-based fallback).
      leftKeys    = jLeftKeys,
      rightKeys   = jRightKeys,
      onExprString = optStringField(modelObj, "onExprString"),
      // Path C: also carry the extra dims/measures so the runtime
      // identity (alias-prefixed dims, etc.) is preserved.
      extraDimensions = jExtraDims,
      extraMeasures   = jExtraMeasures,
      // Path C: prefix fields carry through (caveat §1.3).
      leftPrefix      = jLeftPrefix,
      rightPrefix     = jRightPrefix,
    )

    // If the writer emitted extra dims/measures, attach them via a
    // SemanticTransformsOp wrapper. Each Dimension/Measure becomes a
    // Transform that resolves its column at runtime against the joined
    // DataFrame's scope — the same shape YamlLoader produces.
    val finalRoot: SemanticOp =
      if (jExtraDims.nonEmpty || jExtraMeasures.nonEmpty) {
        val transforms: Seq[Transform] =
          jExtraDims.values.toSeq.map { d =>
            val t: Transform = d.exprString match {
              case Some(expr) =>
                Transform(d.name, _ => org.apache.spark.sql.functions.expr(expr),
                          d.description, exprString = Some(expr))
              case None =>
                Transform(d.name, scope => scope(d.name),
                          d.description, exprString = Some(d.name))
            }
            t
          } ++ jExtraMeasures.values.toSeq.map { m =>
            val t: Transform = m.exprString match {
              case Some(expr) =>
                Transform(m.name, _ => org.apache.spark.sql.functions.expr(expr),
                          m.description, exprString = Some(expr))
              case None =>
                Transform(m.name, scope => scope(m.name),
                          m.description, exprString = Some(m.name))
            }
            t
          }
        SemanticTransformsOp(baseJoin, transforms)
      } else baseJoin

    new SemanticTable(
      root        = finalRoot,
      version     = version,
      sourceTable = None,
      status      = ModelStatus.fromString(joinedStatus).getOrElse(ModelStatus.Published),
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
    digObj.put("transforms",             transformCount(model))
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

    // transforms[] — applied in declaration order; column outputs feed
    // subsequent transforms / dimensions / measures. Each transform's
    // `expr` is the source-string form (or `<lambda>` sentinel if it was
    // built in Scala code without an `exprString`).
    val transforms = collectTransforms(model)
    if (transforms.nonEmpty) {
      val transArr = root.putArray("transforms")
      transforms.foreach { t =>
        val obj = transArr.addObject()
        obj.put("name", t.name)
        obj.put("expr", t.exprString.getOrElse(LambdaSentinel))
        putOptString(obj, "description", t.description)
      }
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
    val transforms = readArr(obj, "transforms").flatMap(readTransform)

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
    // If the manifest carries transforms, wrap the root in a
    // SemanticTransformsOp so they re-apply when the consumer calls
    // `.execute()`. Mirrors how YamlLoader + withTransforms() build the
    // in-memory tree.
    val transformedRoot = if (transforms.nonEmpty)
      SemanticTransformsOp(withFiltersRoot, transforms)
    else
      withFiltersRoot

    new SemanticTable(
      root        = transformedRoot,
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

  /** Parse one transform node. Same pattern as `readMeasure`: the `expr`
    * string is the source expression (e.g. `datediff(shipped_at,
    * order_date)`) and `description` carries metadata. The lambda
    * `_ => expr(exprString)` faithfully re-creates the YAML's behavior
    * via `org.apache.spark.sql.functions.expr(...)`, so transforms
    * round-trip through the wire artifact losslessly as long as the
    * `expr` is a plain SQL expression.
    *
    * Sentinel handling: a `<lambda>` expr (meaning the original was built
    * in Scala without an `exprString`) produces a placeholder column
    * lookup via `org.apache.spark.sql.functions.col(name)`. This keeps
    * the round-trip non-throwing for hand-built transforms — a consumer
    * who needs the original lambda behavior must re-load from the
    * original Scala source. */
  private def readTransform(node: com.fasterxml.jackson.databind.JsonNode): Option[Transform] = {
    val n = optStringField(node, "name")
    val e = optStringField(node, "expr")
    val d = optStringField(node, "description")
    if (n.isEmpty || e.isEmpty) return None
    val name        = n.get
    val exprString  = e.get
    val description = d
    val exprFn: SemanticScope => org.apache.spark.sql.Column =
      if (exprString == LambdaSentinel)
        scope => org.apache.spark.sql.functions.col(name)
      else
        scope => org.apache.spark.sql.functions.expr(exprString)
    Some(Transform(name, exprFn, description, exprString = Some(exprString)))
  }

  /** Walk the op-tree and collect all transforms, in declaration order. */
  private def collectTransforms(model: SemanticTable): Seq[Transform] = {
    val buf = scala.collection.mutable.ListBuffer.empty[Transform]
    def walk(op: SemanticOp): Unit = op match {
      case tr: SemanticTransformsOp   => tr.transforms.foreach(t => buf += t); walk(tr.source)
      case a: SemanticAggregateOp      => walk(a.source)
      case f: SemanticFilterOp         => walk(f.source)
      case rf: SemanticRowFilterOp     => walk(rf.source)
      case o: SemanticOrderByOp        => walk(o.source)
      case l: SemanticLimitOp          => walk(l.source)
      case h: SemanticHintOp           => walk(h.source)
      case j: SemanticJoinOp           => walk(j.left); walk(j.right)
      case _                          => ()
    }
    walk(model.root)
    buf.result()
  }

  /** Count transforms in the model. Mirrors `transformCount` references in
    * the digest block. */
  private def transformCount(model: SemanticTable): Int =
    collectTransforms(model).size

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
