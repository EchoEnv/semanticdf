package io.semanticdf.adapters

import io.semanticdf.{Dimension, Measure, SemanticTable, toSemanticTable}


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.yaml.snakeyaml.Yaml

import java.nio.file.{Files, Path => NioPath}
import scala.jdk.CollectionConverters._

/** Apache Ossie YAML adapter — implements [[SemanticMetadataAdapter]]
  * for the canonical `semantic_model.{datasets,relationships,metrics}`
  * shape defined in `core-spec/spec.yaml` and validated by
  * `core-spec/osi-schema.json` (version `0.2.0.dev0`).
  *
  * == Why Ossie ==
  *
  * Ossie is the Apache-Software-Foundation-hosted (formerly
  * "Open Semantic Interchange") effort to standardise semantic-metadata
  * exchange. Today the canonical format is YAML; ten vendor
  * converters (dbt, gooddata, polaris, snowflake, databricks, omni,
  * honeydew, salesforce, orionbelt, wisdom) already exist in the
  * project. Building against the canonical Ossie shape means a
  * semanticdf model is portable to every other vendor with a
  * one-line converter.
  *
  * == What's in the wire format ==
  *
  * {{{
  *   version: "0.2.0.dev0"
  *   semantic_model:
  *     - name: sales_analytics
  *       datasets:
  *         - name: orders
  *           source: sales.public.orders
  *           fields:
  *             - name: order_date
  *               expression: { dialects: [{dialect: ANSI_SQL, expression: order_date}] }
  *               dimension: {is_time: true}
  *       relationships:
  *         - name: orders_to_customers
  *           from: orders
  *           to: customers
  *           from_columns: [customer_id]
  *           to_columns: [id]
  *       metrics:
  *         - name: total_revenue
  *           expression: { dialects: [{dialect: ANSI_SQL, expression: SUM(orders.amount)}] }
  * }}}
  *
  * == Supported shapes ==
  *
  *   1. **Canonical** (preferred): top-level `semantic_model: [...]`.
  *   2. **Legacy / ontology form** (still in the wild, e.g. the
  *      `flights.yaml` example in the Ossie repo): top-level
  *      `ontology_mappings: [{semantic_model: {...}}, ...]`. The
  *      reader extracts each `semantic_model` and ignores the
  *      `ontology` and `concept_mappings` (those are concept-level
  *      metadata without a first-class semanticdf concept).
  *
  * == What's NOT consumed in v1 ==
  *
  *   - `ai_context` (synonyms, instructions, examples) — preserved
  *     in the intermediate but not yet mapped to semanticdf
  *     `metadata`. Follow-up.
  *   - `custom_extensions` — vendor-specific; ignored.
  *   - The `ontology` block's concept declarations — preserved
  *     verbatim on the project but not used to build tables.
  *   - Multi-dialect expressions: only `ANSI_SQL` (or the first
  *     dialect as fallback) is read. The rest are ignored. A
  *     Snowflake/Databricks dialect map is a v2.
  *   - `primary_key` / `unique_keys` — preserved on the
  *     intermediate but not used to build the SemanticTable.
  *     semanticdf doesn't have a first-class grain concept yet.
  *
  * == Usage ==
  *
  * {{{
  *   import io.semanticdf.adapters.OssieReader._
  *   implicit val spark: SparkSession = ...
  *   val tables = loadSemanticTables(Paths.get("flights.yaml"), resolve)
  * }}} */
object OssieReader extends SemanticMetadataAdapter[NioPath, OssieProject] {

  /** Phase 1 — pure parse. Loads the YAML, walks both the canonical
    * shape (`semantic_model`) and the legacy ontology shape
    * (`ontology_mappings[*].semantic_model`), returns one
    * [[OssieProject]] per `semantic_model` entry. */
  def parse(source: NioPath): Seq[OssieProject] = {
    if (!Files.exists(source))
      throw new IllegalArgumentException(s"Ossie file not found: $source")
    val root = new Yaml().load[java.util.Map[String, Any]](
      Files.newBufferedReader(source))
    val map = Option(root).map(_.asScala.toMap).getOrElse(Map.empty)
    if (!map.contains("version"))
      throw new IllegalArgumentException(
        s"Ossie file missing required 'version' key: $source")
    val ontology: Seq[Map[String, Any]] = map.get("ontology").map(asList).getOrElse(Seq.empty).map(asMap)

    val canonical = map.get("semantic_model").map(asList).getOrElse(Seq.empty)
    val legacy    = map.get("ontology_mappings").map(asList).getOrElse(Seq.empty)
      .flatMap(_.asInstanceOf[java.util.Map[String, Any]].asScala.get("semantic_model"))
      .map(asMap)

    val models = (canonical.map(asMap) ++ legacy.map(asMap)).map(parseSemanticModel)
    models.map(_.copy(ontology = ontology))
  }

  /** Phase 2 — bind to Spark. Builds a `SemanticTable` per
    * `OssieDataset` and wires the relationships. The caller
    * supplies a `resolve(source)` callback that turns each
    * dataset's `source` string into a `DataFrame`. */
  def toSemanticTables(
      projects: Seq[OssieProject],
      resolve:  String => DataFrame,
  )(implicit spark: SparkSession): Map[String, SemanticTable] = {
    if (projects.isEmpty) return Map.empty

    // Single pass: build every dataset as a SemanticTable, then
    // wire relationships after all tables exist (joins reference
    // tables by name, so the target must exist before the join
    // is constructed).
    val tables = scala.collection.mutable.LinkedHashMap.empty[String, SemanticTable]
    projects.foreach { project =>
      project.datasets.foreach { ds =>
        val df  = resolve(ds.source)
        val st  = toSemanticTable(df, name = Some(ds.name), sourceTable = Some(ds.source),
                                description = ds.description)
        val withDims = ds.fields.filter(!_.isTimeDimension).foldLeft(st) { (acc, f) =>
          acc.withDimensions(new Dimension(
            name = f.name,
            expr = (t: io.semanticdf.SemanticScope) => exprFor(f.expression, t, f.name),
            description = f.description,
            exprString = Some(f.expression),
          ))
        }
        val withTime = ds.fields.filter(_.isTimeDimension).foldLeft(withDims) { (acc, f) =>
          acc.withDimensions(Dimension.time(
            name = f.name,
            expr = (t: io.semanticdf.SemanticScope) => exprFor(f.expression, t, f.name),
            description = f.description,
          ))
        }
        val withMeasures = project.metrics.foldLeft(withTime) { (acc, m) =>
          acc.withMeasures(Measure(
            name = m.name,
            expr = (t: io.semanticdf.SemanticScope) => exprFor(m.expression, t, m.name),
            description = m.description,
          ).copy(exprString = Some(m.expression)))
        }
        tables(ds.name) = withMeasures
      }
    }

    // Wire relationships. Each relationship has `from` (the many
    // side) and `to` (the one side) per Ossie's convention. We
    // default cardinality to `one` (Ossie doesn't enumerate
    // many-to-one vs one-to-one; for v1 we treat all relationships
    // as many-to-one which is the common case).
    projects.foreach { project =>
      project.relationships.foreach { rel =>
        val fromTable = tables.get(rel.from)
        val toTable   = tables.get(rel.to)
        (fromTable, toTable) match {
          case (Some(ft), Some(tt)) =>
            // Ossie's `from_columns` / `to_columns` are parallel
            // arrays; for v1 we require same length and use the
            // first column as the symmetric join key (the common
            // case). Composite keys are preserved in the project
            // but not yet wired — a v2 extension.
            if (rel.fromColumns.nonEmpty && rel.fromColumns.length == rel.toColumns.length) {
              val join = ft.join_on(tt, rel.fromColumns.head -> rel.toColumns.head)
              tables(rel.from) = join
            }
            // Asymmetric keys would need an extension; for v1 we
            // skip mismatched or empty relationships.
          case _ =>
            // Relationship references an unknown dataset; skip
            // silently. v1 is permissive on broken metadata.
        }
      }
    }

    tables.toMap
  }

  // ----------------------------------------------------------------
  // Wire-shape parsing helpers
  // ----------------------------------------------------------------

  /** Parse one `semantic_model` map (a single Ossie model). */
  private def parseSemanticModel(sm: Map[String, Any]): OssieProject = {
    val datasets      = sm.get("datasets").map(asList).getOrElse(Seq.empty).map(asMap).map(parseDataset)
    val relationships = sm.get("relationships").map(asList).getOrElse(Seq.empty).map(asMap).map(parseRelationship)
    val metrics       = sm.get("metrics").map(asList).getOrElse(Seq.empty).map(asMap).map(parseMetric)
    OssieProject(
      name        = sm.getOrElse("name", "<unnamed>").toString,
      description = sm.get("description").map(_.toString),
      datasets    = datasets,
      relationships = relationships,
      metrics     = metrics,
    )
  }

  private def parseDataset(ds: Map[String, Any]): OssieDataset = {
    val fields      = ds.get("fields").map(asList).getOrElse(Seq.empty).map(asMap).map(parseField)
    OssieDataset(
      name        = ds.getOrElse("name", "<unnamed>").toString,
      source      = ds.getOrElse("source", "").toString,
      description = ds.get("description").map(_.toString),
      primaryKey  = ds.get("primary_key").map(asList).getOrElse(Seq.empty).map(_.toString),
      uniqueKeys  = ds.get("unique_keys").map(asList).getOrElse(Seq.empty).map { row =>
        asList(row.asInstanceOf[Any]).map(_.toString)
      },
      fields      = fields,
    )
  }

  private def parseField(f: Map[String, Any]): OssieField = {
    val expr        = pickAnsiExpression(f.get("expression"))
    val isTime      = f.get("dimension").map(asMap).flatMap(_.get("is_time").map(_.asInstanceOf[Boolean])).getOrElse(false)
    OssieField(
      name        = f.getOrElse("name", "<unnamed>").toString,
      expression  = expr,
      description = f.get("description").map(_.toString),
      datatype    = f.get("datatype").map(_.toString),
      isTimeDimension = isTime,
    )
  }

  private def parseRelationship(r: Map[String, Any]): OssieRelationship =
    OssieRelationship(
      name        = r.getOrElse("name", "<unnamed>").toString,
      from        = r.getOrElse("from", "").toString,
      to          = r.getOrElse("to", "").toString,
      fromColumns = r.get("from_columns").map(asList).getOrElse(Seq.empty).map(_.toString),
      toColumns   = r.get("to_columns").map(asList).getOrElse(Seq.empty).map(_.toString),
    )

  private def parseMetric(m: Map[String, Any]): OssieMetric =
    OssieMetric(
      name        = m.getOrElse("name", "<unnamed>").toString,
      expression  = pickAnsiExpression(m.get("expression")),
      description = m.get("description").map(_.toString),
      datatype    = m.get("datatype").map(_.toString),
    )

  /** Pick the ANSI_SQL expression from an Ossie `expression` block.
    * Falls back to the first dialect if ANSI_SQL is missing. Returns
    * the column name as a placeholder if no expression is present
    * (e.g. simple column references where the YAML was malformed). */
  private def pickAnsiExpression(expr: Option[Any]): String = {
    val dialects = expr.map(asMap).flatMap(_.get("dialects")).map(asList).getOrElse(Seq.empty)
    // Try ANSI_SQL first.
    val ansi = dialects.collectFirst {
      case d if asMap(d).get("dialect").contains("ANSI_SQL") =>
        asMap(d).get("expression").map(_.toString).getOrElse("")
    }
    ansi.orElse {
      dialects.collectFirst {
        case d => asMap(d).get("expression").map(_.toString)
      }.flatten
    }.getOrElse("")
  }

  // ----------------------------------------------------------------
  // Generic collection coercion helpers (Scala 2.13 + SnakeYAML)
  // ----------------------------------------------------------------

  private def asMap(a: Any): Map[String, Any] = a match {
    case m: java.util.Map[_, _] => m.asScala.toMap.asInstanceOf[Map[String, Any]]
    case m: Map[_, _]          => m.asInstanceOf[Map[String, Any]]
    case other => throw new IllegalArgumentException(
      s"expected YAML map, got ${other.getClass.getSimpleName}: $other")
  }

  private def asList(a: Any): Seq[Any] = a match {
    case jl: java.util.List[_] => jl.asScala.toSeq
    case s: Seq[_]            => s
    case other => throw new IllegalArgumentException(
      s"expected YAML sequence, got ${other.getClass.getSimpleName}: $other")
  }

  /** Build a Spark `Column` from an expression string. Direct column
    * refs (a single identifier) go through the scope's `apply`; any
    * other expression uses Spark's `expr(...)` parser. */
  /** Build a Spark `Column` from an expression string.
    *
    * For dimensions: a bare identifier is a column reference (uses
    * `t(name)`); anything else is a SQL expression (uses `expr(...)`).
    * This is the common case and keeps things fast.
    *
    * For measures: expressions are always aggregates (`SUM(...)`,
    * `COUNT(1)`, etc.). They contain parens and operators, so the
    * identifier regex doesn't match — the `expr(...)` path is taken
    * automatically. We rely on that here rather than building
    * special-cased logic for measures. */
  private def exprFor(
      expression: String,
      t: io.semanticdf.SemanticScope,
      fallbackName: String,
  ): org.apache.spark.sql.Column = {
    if (expression.isEmpty) t(fallbackName)
    else if (expression.matches("[A-Za-z_][A-Za-z_0-9]*")) t(expression)
    else org.apache.spark.sql.functions.expr(stripTablePrefix(expression))
  }

  /** Strip the dataset-name prefix from a metric expression.
    *
    * Ossie metrics reference fields with the dataset name as a
    * prefix: `SUM(orders.amount)`. After the metric is bound to a
    * single dataset, the prefix is redundant and Spark parses the
    * `orders.amount` as a struct field access (which doesn't exist
    * on the actual table). Stripping the prefix leaves a plain
    * `SUM(amount)` that Spark parses correctly.
    *
    * Strips the longest leading identifier + dot sequence. Doesn't
    * touch identifiers inside parens (e.g. table refs in a CTE
    * wouldn't be affected — though Ossie metrics don't use those). */
  // Match an identifier followed by a dot, anywhere in the string.
  // Used to strip dataset-name prefixes from Ossie metric expressions
  // (`SUM(orders.amount)` → `SUM(amount)`) so Spark's expr() parser
  // doesn't interpret `orders.amount` as a struct field access.
  // Karpathy: minimum code. Not a real SQL parser — works for the
  // common case where the table-name prefix matches an identifier.
  private val anyIdentDot = """([A-Za-z_][A-Za-z_0-9]*)\.""".r
  private def stripTablePrefix(expression: String): String =
    anyIdentDot.replaceAllIn(expression, m => {
      val ident = m.group(1)
      // Don't strip SQL keywords or function names
      val sqlKeywords = Set("SUM", "COUNT", "AVG", "MIN", "MAX", "DISTINCT",
        "COALESCE", "CAST", "CASE", "WHEN", "THEN", "ELSE", "END", "AS",
        "AND", "OR", "NOT", "NULL", "IS", "IN", "EXISTS", "BETWEEN",
        "FROM", "WHERE", "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET")
      if (sqlKeywords.contains(ident.toUpperCase)) m.matched
      else ""
    })
}
