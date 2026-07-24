package io.semanticdf

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.{Files, Path => NioPath}
import scala.jdk.CollectionConverters._

/** Adapter for dbt's `manifest.json` — reads the file dbt-core produces via
  * `dbt parse` and turns it into a set of [[SemanticTable]]s.
  *
  * == Why ==
  *
  * dbt users already have a manifest for their warehouse. They don't want
  * to hand-author a second YAML to expose the same models to a semantic
  * layer. This reader closes that gap: dbt schema + meta annotations are
  * the source of truth, the adapter produces the [[SemanticTable]] graph.
  *
  * == How (v0.1.x) ==
  *
  * Reads `manifest.json` v12+ (dbt-core 1.6+). Filters the `nodes` map to
  * `resource_type == "model"`, then for each model:
  *
  *   - `name`        → the [[SemanticTable]]'s model name.
  *   - `alias` (or `name`) → the source table the caller must load.
  *   - `description` → the [[SemanticTable]]'s description.
  *   - `columns[*]`  → dimensions, with one exception:
  *       - A column whose `meta` has a non-empty `kind: measure` AND a
  *         non-empty `expr` field becomes a base measure instead.
  *   - `tags`        → propagated to the model.
  *
  * Joins, exposures, metrics, sources, snapshots, tests, and the
  * `parent_map` / `child_map` dependency graph are **not** consumed in v1.
  * The reader does not infer join keys from the manifest (dbt itself
  * does not record them), so joining requires a separate annotation
  * pass that's better expressed as a follow-up. The data is preserved in
  * [[DbtProject]] so a v2 reader can pick it up without re-parsing.
  *
  * == Wire convention for `meta` ==
  *
  * To mark a column as a measure, the user adds to their `schema.yml`:
  *
  * {{{
  *   models:
  *     - name: orders
  *       columns:
  *         - name: revenue
  *           meta:
  *             kind: measure
  *             expr: "sum(amount)"
  * }}}
  *
  * Anything else stays a dimension. There is **no** `kind: dimension`
  * marker — dimensions are the default. This keeps the v1 annotation
  * surface small: users opt in to measures explicitly.
  *
  * == Usage ==
  *
  * {{{
  *   // Phase 1: read the manifest. No Spark needed.
  *   val project = DbtManifestReader.read(Paths.get("target/manifest.json"))
  *
  *   // Phase 2: bind to a Spark session, resolving each model's source
  *   // table to a DataFrame.
  *   val spark: SparkSession = ...
  *   val tables: Map[String, SemanticTable] = project.toSemanticTables(spark) { model =>
  *     spark.read.format("parquet").load(s"/data/${model.sourceTable}")
  *   }
  * }}} */
object DbtManifestReader {

  /** Per-project result: the parsed manifest, before any Spark binding. */
  final case class DbtProject(
      manifestVersion: Option[String],
      models: Map[String, DbtModel],
      sources: Map[String, DbtSource],
      rawNodes: Map[String, Map[String, Any]],
  )

  /** One dbt model — a `nodes` entry with `resource_type == "model"`.
    *
    * The `sourceTable` is the dbt `alias` (or `name` if no alias). It's
    * expressed as `schema.name` for catalog-bound models, or just
    * `name` for temp-view / file-bound consumers. The caller decides how
    * to resolve it (see [[DbtProject.toSemanticTables]]). */
  final case class DbtModel(
      name:        String,
      sourceTable: String,
      description: Option[String],
      database:    Option[String],
      schema:      Option[String],
      tags:        Seq[String],
      dimensions:  Seq[DbtField],
      measures:    Seq[DbtField],
  )

  /** A source — preserved for v2 readers that want to wire dbt sources
    * to a `StreamingSupport` or a separate input contract. */
  final case class DbtSource(
      name:        String,
      identifier:  Option[String],
      database:    Option[String],
      schema:      Option[String],
      description: Option[String],
  )

  /** One field on a dbt model. `expr` is the Spark SQL expression
    * (a column reference for dimensions, an aggregation for measures). */
  final case class DbtField(
      name:        String,
      expr:        String,
      description: Option[String],
      dataType:    Option[String],
  )

  // ── Public entry: read manifest from disk ──────────────────────

  /** Read a `manifest.json` from a path on disk. Throws on parse error
    * or if the file is missing. */
  def read(manifestPath: NioPath): DbtProject = {
    if (!Files.exists(manifestPath))
      throw new IllegalArgumentException(
        s"manifest file not found: $manifestPath")
    val mapper = newJsonMapper()
    val rootJava = mapper.readValue(
      manifestPath.toFile,
      classOf[java.util.Map[String, Object]],
    )
    val root: Map[String, Any] = rootJava.asScala.toMap.asInstanceOf[Map[String, Any]]
    read(root)
  }

  /** Read from an already-parsed JSON map. Exposed for testing and for
    * callers that already have the manifest in memory. */
  def read(root: Map[String, Any]): DbtProject = {
    val meta        = root.get("metadata").map(asMap)
    val version     = meta.flatMap(_.get("dbt_schema_version").map(_.toString))
    val nodesRaw    = root.get("nodes").map(asMap).getOrElse(Map.empty)
    val sourcesRaw  = root.get("sources").map(asMap).getOrElse(Map.empty)

    val models   = scala.collection.mutable.LinkedHashMap.empty[String, DbtModel]
    val sources  = scala.collection.mutable.LinkedHashMap.empty[String, DbtSource]
    val rawNodes = scala.collection.mutable.LinkedHashMap.empty[String, Map[String, Any]]

    nodesRaw.foreach { case (uid, nodeRaw) =>
      rawNodes(uid) = asMap(nodeRaw)
      val node = asMap(nodeRaw)
      node.get("resource_type") match {
        case Some("model") =>
          val m = parseModel(uid, node)
          models.get(m.name) match {
            case Some(existing) if existing != m =>
              // Two dbt models with the same `name` (different packages) —
              // suffix with the package to disambiguate. We don't merge
              // because dbt users expect one semantic model per dbt model.
              models(s"${packageOf(uid)}.${m.name}") = m
              models.remove(m.name)
            case _ =>
              models(m.name) = m
          }
        case _ => // skip seeds, snapshots, tests, analyses, etc.
      }
    }

    sourcesRaw.foreach { case (uid, sourceRaw) =>
      val s = parseSource(uid, asMap(sourceRaw))
      sources(s.name) = s
    }

    DbtProject(
      manifestVersion = version,
      models   = models.toMap,
      sources  = sources.toMap,
      rawNodes = rawNodes.toMap,
    )
  }

  // ── Per-model parsing ──────────────────────────────────────────

  private def parseModel(uid: String, node: Map[String, Any]): DbtModel = {
    val name        = asString(node, "name").getOrElse(uid)
    val database    = asString(node, "database")
    val schema      = asString(node, "schema")
    val alias       = asString(node, "alias").getOrElse(name)
    val description = asString(node, "description").filter(_.nonEmpty)
    val tags        = asStringSeq(node, "tags")

    val sourceTable = formatSourceTable(database, schema, alias)
    val rawColumns  = node.get("columns").map(asMap).getOrElse(Map.empty)
    val (dims, meas) = partitionColumns(rawColumns)

    DbtModel(
      name        = name,
      sourceTable = sourceTable,
      description = description,
      database    = database,
      schema      = schema,
      tags        = tags,
      dimensions  = dims,
      measures    = meas,
    )
  }

  private def parseSource(uid: String, node: Map[String, Any]): DbtSource = {
    DbtSource(
      name        = asString(node, "name").getOrElse(uid),
      identifier  = asString(node, "identifier"),
      database    = asString(node, "database"),
      schema      = asString(node, "schema"),
      description = asString(node, "source_description").orElse(asString(node, "description")).filter(_.nonEmpty),
    )
  }

  /** Split a dbt `columns` map into dimensions (default) and measures
    * (marked via `meta: { kind: measure, expr: <sql> }`). */
  private def partitionColumns(
      rawColumns: Map[String, Any]
  ): (Seq[DbtField], Seq[DbtField]) = {
    val dims = scala.collection.mutable.ListBuffer.empty[DbtField]
    val meas = scala.collection.mutable.ListBuffer.empty[DbtField]
    rawColumns.foreach { case (_, colRaw) =>
      val col = asMap(colRaw)
      val name = asString(col, "name").getOrElse(return dims.toSeq -> meas.toSeq)
      val expr = asString(col, "expr").getOrElse(name)
      val desc = asString(col, "description").filter(_.nonEmpty)
      val dt   = asString(col, "data_type")
      val meta = col.get("meta").map(asMap).getOrElse(Map.empty)
      val kind = asString(meta, "kind")
      val mExpr = asString(meta, "expr")
      if (kind.contains("measure") && mExpr.exists(_.nonEmpty)) {
        meas += DbtField(name, mExpr.get, desc, dt)
      } else {
        // Default: dimension. Expr is the column name (or a transform the
        // user wrote in dbt's `expr` field, if any).
        dims += DbtField(name, expr, desc, dt)
      }
    }
    dims.toSeq -> meas.toSeq
  }

  // ── Spark binding ──────────────────────────────────────────────

  /** Convert the parsed project into [[SemanticTable]]s. The caller
    * supplies a resolver that maps a `sourceTable` string (e.g.
    * `"public.flights"`) to a Spark [[DataFrame]]. The resolver is
    * invoked once per model.
    *
    * Models with no dimensions and no measures are still emitted as
    * `SemanticTable`s (an empty semantic graph) so the user can introspect
    * them. Models whose `sourceTable` the resolver doesn't recognise
    * raise `IllegalArgumentException` with the full list of unresolved
    * models. */
  def toSemanticTables(
      project: DbtProject,
      spark:   SparkSession,
      resolve: String => DataFrame,
  ): Map[String, SemanticTable] = {
    if (project.models.isEmpty) return Map.empty
    val resolved = scala.collection.mutable.LinkedHashMap.empty[String, SemanticTable]
    val unresolved = scala.collection.mutable.ListBuffer.empty[String]
    project.models.foreach { case (name, m) =>
      val df =
        try resolve(m.sourceTable)
        catch { case _: Exception =>
          unresolved += name
          null
        }
      if (df != null) {
        resolved(name) = buildSemanticTable(m, df)
      }
    }
    if (unresolved.nonEmpty)
      throw new IllegalArgumentException(
        s"Could not resolve source tables for ${unresolved.length} model(s): " +
        unresolved.mkString(", "))
    resolved.toMap
  }

  private def buildSemanticTable(m: DbtModel, df: DataFrame): SemanticTable = {
    val dims = m.dimensions.map { d =>
      new Dimension(
        name = d.name,
        expr = (t: SemanticScope) => expr(t, d.expr),
        description = d.description,
        exprString = Some(d.expr),
      )
    }
    val meas = m.measures.map { ms =>
      Measure(
        name = ms.name,
        expr = (t: SemanticScope) => expr(t, ms.expr),
        description = ms.description,
        exprString = Some(ms.expr),
      )
    }
    toSemanticTable(
      df,
      name = Some(m.name),
      description = m.description,
      sourceTable = Some(m.sourceTable),
    ).withDimensions(dims: _*).withMeasures(meas: _*)
  }

  /** Compile a Spark SQL expression inside a [[SemanticScope]] by
    * resolving it through the scope's `apply`. For plain column refs
    * (no parens, no operators) this is a direct lookup. For expressions
    * like `sum(amount)` or `amount * 0.1`, we wrap with
    * `org.apache.spark.sql.functions.expr`. The scope's `apply` is
    * preferred for column refs so unknown-column errors stay in-library. */
  private def expr(t: SemanticScope, expression: String): org.apache.spark.sql.Column = {
    val isColumnRef = expression.matches("[A-Za-z_][A-Za-z_0-9.]*")
    if (isColumnRef) t(expression)
    else org.apache.spark.sql.functions.expr(expression)
  }

  // ── Helpers ────────────────────────────────────────────────────

  /** Render a `sourceTable` string. dbt writes `database` / `schema` /
    * `alias` as separate fields. We assemble a fully-qualified name when
    * the dbt project uses a catalog (so callers can `spark.table(...)`
    * with one string); otherwise we just emit the alias. */
  private def formatSourceTable(
      database: Option[String],
      schema:   Option[String],
      alias:    String,
  ): String = (database, schema) match {
    case (Some(db), Some(sc)) if db.nonEmpty && sc.nonEmpty => s"$db.$sc.$alias"
    case (_, Some(sc))            if sc.nonEmpty            => s"$sc.$alias"
    case _                                                  => alias
  }

  /** `model.<package>.<name>` → package. Returns `""` when the uid is
    * not a node unique id. */
  private def packageOf(uid: String): String = uid.split('.').dropRight(1).mkString(".")

  private def asMap(a: Any): Map[String, Any] = a match {
    case m: java.util.Map[_, _] => m.asScala.toMap.asInstanceOf[Map[String, Any]]
    case m: Map[_, _]          => m.asInstanceOf[Map[String, Any]]
    case other => throw new IllegalArgumentException(
      s"expected JSON object, got ${other.getClass.getSimpleName}: $other")
  }

  private def asString(m: Map[String, Any], key: String): Option[String] = m.get(key) match {
    case Some(s: String) => Some(s)
    case Some(n: Number) => Some(n.toString)
    case Some(b: java.lang.Boolean) => Some(b.toString)
    case _ => None
  }

  private def asStringSeq(m: Map[String, Any], key: String): Seq[String] = m.get(key) match {
    case None                => Seq.empty
    case Some(xs: Seq[_])    => xs.collect { case s: String => s }
    case Some(jl: java.util.List[_]) => jl.asScala.collect { case s: String => s }.toSeq
    case _ => Seq.empty
  }

  private def newJsonMapper(): ObjectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
}
