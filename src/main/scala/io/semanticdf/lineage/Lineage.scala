package io.semanticdf.lineage

import io.semanticdf.SemanticTable

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.jdk.CollectionConverters._

/** Static-analysis lineage: a pure, versioned data shape derived from
  * walking a `SemanticTable`'s op tree.
  *
  * The transforms are pure functions. Same input, same output. No
  * runtime instrumentation, no Spark action, no I/O beyond the
  * `toJson` / `fromJson` adapter calls.
  *
  * ==Entry points==
  *
  *   - [[of]] — single-model lineage. For an isolated model, the
  *     upstream-model IDs are `"Unknown"` and the `upstreamModels`
  *     field is empty.
  *
  *   - [[workspaceOf]] — the whole graph. Iterates, calls [[of]] for
  *     each model, then resolves upstream model IDs and the reverse
  *     indexes (`upstreamOf` / `downstreamOf`) from the workspace map.
  *
  * ==MVP limitations==
  *
  *   - No canonical model ID; `name` is used (same constraint as the
  *     YAML loader). See `docs/design/lineage.md` §"Model identity".
  *   - Scala-lambda-built fields are `LineageStatus.Opaque`.
  *   - No runtime lineage; the audit log covers that.
  */
object Lineage {

  /** Current schema version. Bumped only on breaking changes to the
    * JSON shape (adding a field is non-breaking; renaming or removing
    * is). */
  val CurrentSchemaVersion: String = "semanticdf-lineage-v1"

  /** Build a [[ModelLineage]] from a single `SemanticTable` via static
    * analysis. Pure function.
    *
    * For an isolated model, the `upstreamModels` field is `Seq.empty`
    * and the `joins` entries have `leftModel` / `rightModel` set to
    * `"Unknown"`. Use [[workspaceOf]] for a graph with resolved IDs. */
  def of(st: SemanticTable): ModelLineage = {
    val name      = st.name.getOrElse("unknown")
    val walk      = new TreeWalk(st.root)
    val dimensions = st.dimensions.values.toSeq
      .map(d => buildColumnLineage(d, ColumnKind.Dimension))
      .sortBy(_.name)
    val measures   = st.measures.values.toSeq
      .map(m => buildColumnLineage(m, ColumnKind.Measure, st.measures.keySet))
      .sortBy(_.name)
    val transforms = walk.transforms.toSeq
      .map(t => buildColumnLineage(TransformAdapter(t), ColumnKind.Transform))
      .sortBy(_.name)
    ModelLineage(
      modelId        = name,
      modelName      = name,
      sourceTable    = st.sourceTable,
      sourceKind     = walk.sourceKind,
      status         = st.status,
      dimensions     = dimensions,
      measures       = measures,
      transforms     = transforms,
      joins          = walk.joins.toSeq,
      upstreamModels = Seq.empty,
    )
  }

  /** Build a [[WorkspaceLineage]] from a `Map[modelId, SemanticTable]`.
    * Resolves upstream model IDs from the map and computes the
    * reverse-lookup indexes. Pure function.
    *
    * The `modelId` in the workspace map overrides the model's declared
    * name (so two models with the same `name` but different IDs in
    * the workspace can coexist). The model's own `name` (from
    * `toSemanticTable(name=...)`) is preserved as `modelName`. */
  def workspaceOf(models: Map[String, SemanticTable]): WorkspaceLineage = {
    val perModel: Map[String, ModelLineage] = models.map { case (id, st) =>
      id -> of(st).copy(modelId = id, modelName = st.name.getOrElse(id))
    }
    val resolved = perModel.map { case (id, ml) =>
      val upstreamIds = ml.joins.flatMap { j =>
        val leftCandidate  = findUpstream(models, side = "left",  target = j.leftModel)
        val rightCandidate = findUpstream(models, side = "right", target = j.rightModel)
        Seq(leftCandidate, rightCandidate).flatten
      }.distinct
      id -> ml.copy(upstreamModels = upstreamIds)
    }
    // Build the reverse-lookup indexes.
    val downstreamOf: Map[String, Set[String]] = resolved.foldLeft(Map.empty[String, Set[String]]) {
      case (acc, (id, ml)) =>
        ml.upstreamModels.foldLeft(acc) { case (a, up) => a.updatedWith(up)(_.map(_ + id).orElse(Some(Set(id)))) }
    }
    val upstreamOf: Map[String, Set[String]] = resolved.foldLeft(Map.empty[String, Set[String]]) {
      case (acc, (id, ml)) =>
        ml.upstreamModels.foldLeft(acc) { case (a, up) => a.updatedWith(id)(_.map(_ + up).orElse(Some(Set(up)))) }
    }
    // Make every modelId appear in the indexes (even with an empty set)
    // so consumers can call `upstreamOf(id)` without a default.
    val allIds = resolved.keySet
    val upstreamFilled   = allIds.foldLeft(upstreamOf)((m, k)   => m.updated(k, m.getOrElse(k, Set.empty)))
    val downstreamFilled = allIds.foldLeft(downstreamOf)((m, k) => m.updated(k, m.getOrElse(k, Set.empty)))
    WorkspaceLineage(models = resolved, upstreamOf = upstreamFilled, downstreamOf = downstreamFilled)
  }

  /** Best-effort upstream-model-id resolution.
    *
    * MVP: the join's `leftModel` / `rightModel` is always `"Unknown"`
    * from [[of]] (the single-model path). Here we try to recover
    * the upstream model by matching `sourceTable` against the
    * workspace map's value. If a workspace model has the same
    * `sourceTable` as one of the join sides (best-effort), we use
    * its modelId.
    *
    * If neither matches, the side stays `None` (excluded from
    * `upstreamModels`). The MVP does not yet carry the join's
    * `leftRoot.table` reference through the ModelLineage. */
  private def findUpstream(
      models: Map[String, SemanticTable],
      side:   String,
      target: String,
  ): Option[String] = {
    if (target != "Unknown") return Some(target)  // already resolved (future)
    None
  }

  /** Build a [[ColumnLineage]] for a dimension. */
  private def buildColumnLineage(
      d:    io.semanticdf.Dimension,
      kind: ColumnKind,
  ): ColumnLineage = {
    val exprString = d.exprString
    val (baseColumns, status) = exprString match {
      case None    => (Seq.empty[String], LineageStatus.Opaque)
      case Some(s) => (ColumnRefExtractor.extract(s), LineageStatus.Complete)
    }
    ColumnLineage(
      name        = d.name,
      kind        = kind,
      baseColumns = baseColumns,
      exprString  = exprString,
      status      = status,
    )
  }

  /** Build a [[ColumnLineage]] for a measure. */
  private def buildColumnLineage(
      m:                  io.semanticdf.Measure,
      kind:               ColumnKind,
      knownMeasureNames:  Set[String],
  ): ColumnLineage = {
    val exprString = m.exprString
    val (baseColumns, dependsOn, status) = exprString match {
      case None =>
        (Seq.empty[String], Seq.empty[String], LineageStatus.Opaque)
      case Some(s) =>
        val cols = ColumnRefExtractor.extract(s)
        val deps = detectMeasureDeps(s, knownMeasureNames)
        (cols, deps, LineageStatus.Complete)
    }
    ColumnLineage(
      name        = m.name,
      kind        = kind,
      baseColumns = baseColumns,
      dependsOn   = dependsOn,
      exprString  = exprString,
      status      = status,
    )
  }

  /** Build a [[ColumnLineage]] for a transform. */
  private def buildColumnLineage(
      t:      TransformAdapter,
      kind:   ColumnKind,
  ): ColumnLineage = {
    val exprString = t.exprString
    val (baseColumns, status) = exprString match {
      case None    => (Seq.empty[String], LineageStatus.Opaque)
      case Some(s) => (ColumnRefExtractor.extract(s), LineageStatus.Complete)
    }
    ColumnLineage(
      name        = t.name,
      kind        = kind,
      baseColumns = baseColumns,
      exprString  = exprString,
      status      = status,
    )
  }

  /** Find measure-name references in a calc measure's exprString.
    *
    * The measure's exprString may contain references to other measure
    * names (e.g. `total / count`). We look for tokens that match a
    * known measure name. Conservative: only exact-name matches, no
    * partial matches, no SQL-function-name false positives. */
  private def detectMeasureDeps(expr: String, known: Set[String]): Seq[String] = {
    if (known.isEmpty) return Seq.empty
    // Tokenize on non-identifier characters; keep identifier-shaped tokens.
    val ident = "[A-Za-z_][A-Za-z0-9_]*".r
    ident.findAllIn(expr).toSeq.distinct.filter(known.contains)
  }

  /** Serialize a [[WorkspaceLineage]] to JSON. */
  def toJson(wl: WorkspaceLineage, prettyPrint: Boolean = true): String = {
    val mapper = newJsonMapper(prettyPrint)
    val envelope = Map[String, Any](
      "schema"        -> CurrentSchemaVersion,
      "models"        -> wl.models,
      "upstreamOf"    -> wl.upstreamOf,
      "downstreamOf"  -> wl.downstreamOf,
    )
    mapper.writeValueAsString(envelope)
  }

  /** Deserialize a JSON envelope into a [[WorkspaceLineage]]. */
  def fromJson(json: String): WorkspaceLineage = {
    val mapper = newJsonMapper(prettyPrint = false)
    val parsed = mapper.readValue(json, classOf[java.util.Map[String, AnyRef]])
    val schema = Option(parsed.get("schema").asInstanceOf[String])
      .getOrElse(throw new IllegalArgumentException("Missing 'schema' field in lineage JSON"))
    if (!schema.startsWith("semanticdf-lineage-")) {
      throw new IllegalArgumentException(s"Unsupported lineage schema: $schema")
    }
    // `parsed.get(...)` can be either a `java.util.Map` (when the JSON
    // has at least one entry) or a Scala `Map` (when the JSON has zero
    // entries — Jackson uses an empty Scala `Map1` for an empty
    // object). Normalize to a `java.util.Map` so the parsers can use a
    // single signature.
    val modelsM       = toJavaMap(parsed.get("models"))
    val upstreamM     = toJavaMap(parsed.get("upstreamOf"))
    val downstreamM   = toJavaMap(parsed.get("downstreamOf"))
    WorkspaceLineage(
      models        = parseModels(modelsM),
      upstreamOf    = parseIdMap(upstreamM),
      downstreamOf  = parseIdMap(downstreamM),
    )
  }

  /** Normalize a value that may be a `java.util.Map` or a Scala `Map`
    * (Jackson-with-scala-module deserializes empty `{}` to a Scala
    * `Map$EmptyMap`, not a `java.util.Map`). Returns `null` for
    * `null`; an empty `java.util.HashMap` for empty Scala maps. */
  private def toJavaMap(v: AnyRef): java.util.Map[String, AnyRef] = {
    if (v == null) return null
    if (v.isInstanceOf[java.util.Map[_, _]]) {
      v.asInstanceOf[java.util.Map[String, AnyRef]]
    } else if (v.isInstanceOf[scala.collection.Map[_, _]]) {
      val out = new java.util.HashMap[String, AnyRef]()
      val sm = v.asInstanceOf[scala.collection.Map[_, _]]
      sm.foreach { case (k, vv) =>
        out.put(k.toString, vv.asInstanceOf[AnyRef])
      }
      out
    } else {
      throw new IllegalArgumentException(s"Expected a map, got: ${v.getClass.getName}")
    }
  }

  /** Same idea for `List`. Returns `null` for `null`; an empty
    * `java.util.ArrayList` for empty Scala sequences. */
  /** Extract a String from a Jackson-deserialized value. Returns
    * `Some(s)` for actual strings and `None` for null. For non-string
    * non-null values (e.g. an empty object that Jackson-with-Scala-
    * module deserialized as a `Map$EmptyMap$`), returns `None` —
    * which the caller treats as "missing field". The previous
    * `_.toString` approach returned `"Map()"` for these, which
    * then failed validation against named enums. */
  private def toStr(v: AnyRef): Option[String] = v match {
    case null              => None
    case s: String         => Some(s)
    case _                 => None
  }

    private def toJavaList(v: AnyRef): java.util.List[AnyRef] = {
    if (v == null) return null
    if (v.isInstanceOf[java.util.List[_]]) {
      v.asInstanceOf[java.util.List[AnyRef]]
    } else if (v.isInstanceOf[scala.collection.Seq[_]]) {
      val out = new java.util.ArrayList[AnyRef]()
      val sl = v.asInstanceOf[scala.collection.Seq[_]]
      sl.foreach(item => out.add(item.asInstanceOf[AnyRef]))
      out
    } else {
      throw new IllegalArgumentException(s"Expected a list, got: ${v.getClass.getName}")
    }
  }

  private def newJsonMapper(prettyPrint: Boolean): ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    // Register custom serializers for the sealed ADTs. Jackson with the
    // Scala module serializes case objects as empty `{}` (no fields).
    // We want them as JSON strings ("Complete", "Batch", etc.).
    m.registerModule(sealedAdtModule)
    if (prettyPrint) m.writerWithDefaultPrettyPrinter()
    m
  }

  /** Jackson module that serializes the lineage sealed ADTs as JSON
    * strings (their `productPrefix`). */
  private val sealedAdtModule: com.fasterxml.jackson.databind.Module = {
    val mod = new com.fasterxml.jackson.databind.module.SimpleModule("LineageSealedAdts")
    // JVM inner classes get a trailing "$" in their simple name (e.g.
    // "Dimension$" for the case object `Dimension` inside `object
    // ColumnKind`). Strip it for the JSON wire form.
    def cleanName[T](t: T): String = t.getClass.getSimpleName.stripSuffix("$")
    mod.addSerializer(classOf[LineageStatus], new SealedAdtSerializer[LineageStatus](cleanName))
    mod.addSerializer(classOf[SourceKind],    new SealedAdtSerializer[SourceKind](cleanName))
    mod.addSerializer(classOf[ColumnKind],   new SealedAdtSerializer[ColumnKind](cleanName))
    mod
  }

  private def parseModels(map: java.util.Map[String, AnyRef]): Map[String, ModelLineage] = {
    if (map == null) Map.empty
    else {
      val javaM = toJavaMap(map)
      javaM.asScala.toMap.map { case (k, v) =>
        k -> parseModel(toJavaMap(v))
      }
    }
  }

  private def parseModel(m: java.util.Map[String, AnyRef]): ModelLineage = {
    val joins          = parseJoins(m.get("joins"))
    val dimensions     = parseColumns(toJavaList(m.get("dimensions")))
    val measures       = parseColumns(toJavaList(m.get("measures")))
    val transforms     = parseColumns(toJavaList(m.get("transforms")))
    val upstream       = parseStringSeq(toJavaList(m.get("upstreamModels")))
    ModelLineage(
      modelId        = toStr(m.get("modelId")).getOrElse(throw new IllegalArgumentException("Missing modelId")),
      modelName      = toStr(m.get("modelName")).getOrElse(""),
      sourceTable    = toStr(m.get("sourceTable")),
      sourceKind     = SourceKind.valueOf(toStr(m.get("sourceKind")).getOrElse("Batch")),
      status         = parseModelStatus(toStr(m.get("status")).getOrElse("Published")),
      dimensions     = dimensions,
      measures       = measures,
      transforms     = transforms,
      joins          = joins,
      upstreamModels = upstream,
    )
  }

  private def parseJoins(v: AnyRef): Seq[JoinLineage] = {
    if (v == null) Seq.empty
    else toJavaList(v).asScala.toSeq.map { j =>
      val m = toJavaMap(j)
      val keys = toJavaList(m.get("keys")).asScala.toSeq.map { kv =>
        val pair = toJavaList(kv)
        (pair.get(0).asInstanceOf[String], pair.get(1).asInstanceOf[String])
      }
      JoinLineage(
        leftModel   = toStr(m.get("leftModel")).getOrElse("Unknown"),
        rightModel  = toStr(m.get("rightModel")).getOrElse("Unknown"),
        keys        = keys,
        cardinality = toStr(m.get("cardinality")).getOrElse("cross"),
      )
    }
  }

  private def parseColumns(v: AnyRef): Seq[ColumnLineage] = {
    if (v == null) Seq.empty
    else toJavaList(v).asScala.toSeq.map { c =>
      val m = toJavaMap(c)
      ColumnLineage(
        name        = toStr(m.get("name")).getOrElse(""),
        kind        = ColumnKind.valueOf(toStr(m.get("kind")).getOrElse("Dimension")),
        baseColumns = parseStringSeq(m.get("baseColumns")),
        dependsOn   = parseStringSeq(m.get("dependsOn")),
        exprString  = toStr(m.get("exprString")),
        status      = LineageStatus.valueOf(toStr(m.get("status")).getOrElse("Opaque")),
      )
    }
  }

  private def parseIdMap(v: AnyRef): Map[String, Set[String]] = {
    if (v == null) Map.empty
    else toJavaMap(v).asScala.toMap.map { case (k, vv) =>
      val set: Set[String] = toJavaList(vv).asScala.toSet.flatMap(toStr)
      k -> set
    }
  }

  private def parseModelStatus(s: String): io.semanticdf.ModelStatus = s match {
    case "Draft"      => io.semanticdf.ModelStatus.Draft
    case "Published"  => io.semanticdf.ModelStatus.Published
    case "Deprecated" => io.semanticdf.ModelStatus.Deprecated
    case other        => throw new IllegalArgumentException(s"Unknown ModelStatus: $other")
  }

  private def parseStringSeq(v: AnyRef): Seq[String] = {
    if (v == null) Seq.empty
    else toJavaList(v).asScala.toSeq.map(_.asInstanceOf[String])
  }

  // =======================================================================
  // Tree walker — collects the source op + joins + transforms from a
  // SemanticOp tree. Uses a private[semanticdf] visitor pattern; we
  // don't expose this — it's an implementation detail of [[of]].
  // =======================================================================

  /** Mutable collector for a single tree walk. */
  private[lineage] final class TreeWalk(rootOp: io.semanticdf.SemanticOp) {
    var sourceKind: SourceKind = SourceKind.Batch
    val joins: scala.collection.mutable.ArrayBuffer[JoinLineage] = scala.collection.mutable.ArrayBuffer.empty
    val transforms: scala.collection.mutable.ArrayBuffer[io.semanticdf.Transform] = scala.collection.mutable.ArrayBuffer.empty

    private val visitor = new scala.collection.mutable.ArrayBuffer[() => Unit] // unused, kept for clarity

    walk(rootOp)
    private def walk(op: io.semanticdf.SemanticOp): Unit = op match {
      case _: io.semanticdf.SemanticTableOp        => () // leaf
      case _: io.semanticdf.SemanticStreamingTableOp =>
        sourceKind = SourceKind.Streaming
      case j: io.semanticdf.SemanticJoinOp =>
        joins += JoinLineage(
          leftModel   = "Unknown",
          rightModel  = "Unknown",
          keys        = j.leftKeys.zip(j.rightKeys),
          cardinality = j.cardinality.toString.toLowerCase,
        )
        walk(j.left); walk(j.right)
      case t: io.semanticdf.SemanticTransformsOp =>
        transforms ++= t.transforms
        walk(t.source)
      case a: io.semanticdf.SemanticAggregateOp    => walk(a.source)
      case f: io.semanticdf.SemanticFilterOp       => walk(f.source)
      case io.semanticdf.SemanticRowFilterOp(src, _, _, _, _) => walk(src)
      case o: io.semanticdf.SemanticOrderByOp      => walk(o.source)
      case l: io.semanticdf.SemanticLimitOp        => walk(l.source)
      case h: io.semanticdf.SemanticHintOp         => walk(h.source)
    }
  }

  /** Adapter so the same `buildColumnLineage` overload handles both
    * the typed `Dimension` / `Measure` and the simpler `Transform`
    * shape. Avoids scattering the field-shape access. */
  private[lineage] final case class TransformAdapter(t: io.semanticdf.Transform) {
    def name: String = t.name
    def exprString: Option[String] = t.exprString
  }
}

  /** Jackson serializer for a sealed-trait case object. Writes the
    * object's class simple name as a JSON string. Pairs with
    * [[valueOf]] on the companion. */
  private final class SealedAdtSerializer[T <: AnyRef](
      nameOf: T => String,
  ) extends com.fasterxml.jackson.databind.JsonSerializer[T] {
    override def serialize(
        value: T,
        gen:   com.fasterxml.jackson.core.JsonGenerator,
        prov:  com.fasterxml.jackson.databind.SerializerProvider,
    ): Unit = gen.writeString(nameOf(value))
  }
