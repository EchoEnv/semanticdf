package io.semantica

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr
import org.yaml.snakeyaml.Yaml

import scala.jdk.CollectionConverters._

/** Declarative YAML model loader — defines semantic models in a config file, for
  * consumers who know YAML but not Scala.
  *
  * This is the Spark/JVM counterpart of BSL's `from_yaml`. Both coexist with the
  * compiled Scala DSL — the YAML loader builds the exact same `Dimension` / `Measure`
  * / `SemanticTable` objects, so every feature (joins, calcs, percent-of-total,
  * filtering, time grains) works identically.
  *
  * == YAML schema ==
  *
  * {{{
  * # Each top-level key (except reserved ones) is a model name.
  * flights:
  *   table: flights_tbl                 # required: name of the source DataFrame
  *   description: "Flight data"
  *
  *   dimensions:
  *     carrier: carrier                  # shorthand: a single column reference
  *     origin:
  *       expr: origin                    # full form
  *       description: "Origin airport"
  *     dep_time:
  *       expr: dep_time
  *       is_time_dimension: true
  *       smallest_time_grain: day
  *
  *   measures:                           # base aggregates (Spark SQL expressions)
  *     flight_count: "count(1)"
  *     total_distance: "sum(distance)"
  *     avg_distance:
  *       expr: "avg(distance)"
  *       description: "Average flight distance"
  *
  *   calculated_measures:               # calcs reference measures by name
  *     avg_per_flight: "total_distance / flight_count"
  *     pct_of_total: "total_distance / all(total_distance) * 100"
  *
  *   joins:
  *     carriers:                         # alias
  *       model: carriers                 # another model in this YAML
  *       type: one                       # one | many | cross
  *       left_on: carrier                # equi-join key on this model's side
  *       right_on: code                  # equi-join key on the joined model's side
  * }}}
  *
  * == Loading ==
  *
  * {{{
  * // Option A: supply DataFrames explicitly (e.g. from spark.read.parquet)
  * val models = YamlLoader.load("flights.yml", Map(
  *   "flights_tbl"  -> spark.read.parquet("s3://bucket/flights"),
  *   "carriers_tbl" -> spark.read.parquet("s3://bucket/carriers"),
  * ))
  *
  * // Option B: resolve table names from the Spark metastore / catalog
  * val models = YamlLoader.load("flights.yml", spark)
  *
  * val flights = models("flights")
  * flights.groupBy("carrier").aggregate("avg_per_flight").execute(spark)
  * }}}
  *
  * == Expression formats ==
  *
  * - **Dimensions**: a simple column name (`carrier: carrier`) or a Spark SQL
  *   expression (`full_name: "concat(first_name, ' ', last_name")`).
  * - **Measures** (base): a Spark SQL aggregate (`sum(distance)`, `count(1)`).
  * - **Calculated measures**: arithmetic over measure names and `all(name)` for
  *   percent-of-total. Parsed by [[CalcExpr]] so the framework's base-vs-calc
  *   classification works identically to the Scala DSL.
  */
object YamlLoader {

  /** Load semantic models from a YAML file, resolving `table:` references against a
    * caller-supplied map of DataFrames.
    *
    * Use this when your source tables come from `spark.read.parquet(...)`, JDBC, or
    * any non-metastore source.
    *
    * @param path   path to the YAML model definition file
    * @param tables map of table-name → DataFrame, keyed by the `table:` field in YAML
    * @return map of model-name → [[SemanticTable]]
    */
  def load(path: String, tables: Map[String, DataFrame]): Map[String, SemanticTable] =
    buildModels(parseYaml(path), name =>
      tables.getOrElse(name, throw tableNotFound(name, tables.keys)))

  /** Load semantic models from a YAML file, resolving `table:` references against the
    * Spark metastore / catalog via `spark.table(name)`.
    *
    * Use this when your source tables are registered in a Hive metastore, Glue
    * catalog, or `spark.sql("CREATE TABLE ...")`.
    *
    * @param path  path to the YAML model definition file
    * @param spark active SparkSession with access to the metastore
    * @return map of model-name → [[SemanticTable]]
    */
  def load(path: String, spark: SparkSession): Map[String, SemanticTable] =
    buildModels(parseYaml(path), name => spark.table(name))

  // -------------------------------------------------------------------------
  // Parsing
  // -------------------------------------------------------------------------

  private def parseYaml(path: String): Map[String, Map[String, Any]] = {
    val yaml = new Yaml()
    // Use the loan pattern to guarantee the FileInputStream is closed even if
    // parsing throws. SnakeYAML does NOT close the stream itself — leaving it
    // open would leak a file descriptor per load on a long-lived Spark driver.
    val raw = {
      val stream = new java.io.FileInputStream(path)
      try yaml.load[java.util.Map[String, AnyRef]](stream)
      finally stream.close()
    }
    if (raw == null)
      throw new IllegalArgumentException(s"YAML file '$path' is empty or could not be parsed.")
    raw.asScala.view.mapValues(_.asInstanceOf[AnyRef]).toMap.map { case (k, v) =>
      k -> toScalaMap(v)
    }
  }

  /** Recursively convert SnakeYAML's java.util.LinkedHashMap nests to Scala Maps. */
  private def toScalaMap(v: AnyRef): Map[String, Any] = v match {
    case jm: java.util.Map[_, _] =>
      jm.asScala.view.map { case (k, valv) =>
        k.toString -> toScalaVal(valv.asInstanceOf[AnyRef])
      }.toMap
    case other =>
      throw new IllegalArgumentException(
        s"Expected a YAML mapping but got ${other.getClass.getSimpleName}: $other")
  }

  /** Convert nested java.util.Map / java.util.List to Scala equivalents, leaving
    * scalars (String, Int, Boolean, etc.) untouched. */
  private def toScalaVal(v: AnyRef): Any = v match {
    case jm: java.util.Map[_, _] =>
      jm.asScala.view.map { case (k, vv) => k.toString -> toScalaVal(vv.asInstanceOf[AnyRef]) }.toMap
    case jl: java.util.List[_] =>
      jl.asScala.map(e => toScalaVal(e.asInstanceOf[AnyRef])).toSeq
    case null => null
    case other => other
  }

  // -------------------------------------------------------------------------
  // Model building
  // -------------------------------------------------------------------------

  /** Build all models in two passes: (1) create base models with dims/measures,
    * (2) apply joins (which reference other models by name). */
  private def buildModels(
      config: Map[String, Map[String, Any]],
      resolveTable: String => DataFrame,
  ): Map[String, SemanticTable] = {
    // Pass 1: create all models (no joins yet).
    var models = Map.empty[String, SemanticTable]
    config.foreach { case (name, cfg) =>
      models = models.updated(name, buildOneModel(name, cfg, resolveTable))
    }

    // Pass 2: apply joins in declaration order.
    config.foreach { case (name, cfg) =>
      cfg.get("joins").foreach { joinsRaw =>
        val joins = asStringMap(joinsRaw, s"model '$name' joins")
        models = models.updated(name, applyJoins(models(name), joins, models, name))
      }
    }
    models
  }

  /** Build a single model (table + dimensions + base measures + calc measures).
    * Joins are NOT applied here (they need all models to exist first). */
  private def buildOneModel(
      name: String,
      cfg: Map[String, Any],
      resolveTable: String => DataFrame,
  ): SemanticTable = {
    val tableName = cfg.get("table") match {
      case Some(t: String) => t
      case _ => throw new IllegalArgumentException(
        s"Model '$name' must specify a 'table' field (the source DataFrame name).")
    }
    val description = cfg.get("description").collect { case s: String => s }
    val df = resolveTable(tableName)

    var model = toSemanticTable(df, name = Some(name), description = description)

    cfg.get("dimensions").foreach { dimsRaw =>
      val dims = asStringToAnyMap(dimsRaw, s"model '$name' dimensions")
      model = model.withDimensions(dims.map { case (dName, dCfg) =>
        buildDimension(dName, dCfg)
      }.toSeq: _*)
    }

    cfg.get("measures").foreach { measuresRaw =>
      val measures = asStringToAnyMap(measuresRaw, s"model '$name' measures")
      model = model.withMeasures(measures.map { case (mName, mCfg) =>
        buildBaseMeasure(mName, mCfg)
      }.toSeq: _*)
    }

    cfg.get("calculated_measures").foreach { calcsRaw =>
      val calcs = asStringToAnyMap(calcsRaw, s"model '$name' calculated_measures")
      model = model.withMeasures(calcs.map { case (mName, mCfg) =>
        buildCalcMeasure(mName, mCfg)
      }.toSeq: _*)
    }

    model
  }

  // -------------------------------------------------------------------------
  // Dimension / measure builders
  // -------------------------------------------------------------------------

  /** Build a [[Dimension]] from YAML config.
    *
    * - Simple column name → scope-based resolution (`t => t("col")`) so the framework's
    *   resolvability probing (join pre-agg) produces clean errors.
    * - Complex SQL expression → `functions.expr(sql)` (derived dimensions).
    */
  private def buildDimension(name: String, cfg: Any): Dimension = {
    val (exprStr, description, extra) = parseMetricConfig(cfg, "dimension", name)
    val isTimeDim = extra.getOrElse("is_time_dimension", false).asInstanceOf[Boolean]
    val isEntity = extra.getOrElse("is_entity", false).asInstanceOf[Boolean]
    val smallestGrain = extra.get("smallest_time_grain").map(_.toString)

    if (isTimeDim)
      Dimension.time(name, dimensionExpr(exprStr), smallestTimeGrain = smallestGrain,
        description = description)
    else if (isEntity)
      Dimension.entity(name, dimensionExpr(exprStr), description)
    else
      Dimension(name, dimensionExpr(exprStr), description)
  }

  /** A dimension lambda: simple identifiers go through the scope (for clean
    * resolvability probing); complex SQL expressions use `functions.expr`. */
  private def dimensionExpr(exprStr: String): SemanticScope => org.apache.spark.sql.Column =
    if (exprStr.matches("[a-zA-Z_][a-zA-Z0-9_.]*"))
      t => t(exprStr)
    else
      _ => expr(exprStr)

  /** Build a base [[Measure]] from the `measures:` section.
    *
    * Uses `functions.expr(sql)` for the actual Spark SQL aggregate (e.g. `sum(distance)`),
    * BUT the lambda first probes each column reference through the scope. This is
    * critical for join pre-aggregation: the framework tests whether a measure resolves
    * on each side by calling `m.expr(scope)` and catching `UnknownFieldError`. A bare
    * `_ => expr(sql)` would NOT throw (Spark's `expr()` is lazy), so every base measure
    * would be considered resolvable on both sides of a join → duplicate columns.
    * Probing column refs through the scope makes the resolvability test accurate. */
  private def buildBaseMeasure(name: String, cfg: Any): Measure = {
    val (exprStr, description, extra) = parseMetricConfig(cfg, "measure", name)
    val metadata = extra.get("metadata").map(_.asInstanceOf[Map[String, String]].map { case (k, v) =>
      k -> v.toString
    }).getOrElse(Map.empty[String, String])
    val colRefs = extractColumnRefs(exprStr)
    Measure(name, t => {
      colRefs.foreach(c => t(c))  // throws UnknownFieldError if a column is missing
      expr(exprStr)
    }, description, metadata)
  }

  /** Extract probable column-reference identifiers from a Spark SQL expression string,
    * filtering out SQL keywords, aggregate function names, and numeric literals.
    *
    * Used by [[buildBaseMeasure]] so the join pre-agg probe can tell whether a measure
    * resolves on a given DataFrame. This is intentionally conservative: it only needs
    * to be correct about which identifiers ARE columns, not which aren't (a false
    * positive — treating a function name as a column — would cause a spurious throw,
    * but the blocklist covers all standard aggregates). */
  private def extractColumnRefs(sql: String): Seq[String] = {
    val sqlKeywords = Set(
      // aggregate / window functions
      "sum", "count", "avg", "mean", "min", "max", "stddev", "variance", "first", "last",
      // other functions commonly used in measure expressions
      "coalesce", "cast", "round", "floor", "ceil", "abs", "distinct", "over", "partition",
      // SQL clauses / types that may appear in casts
      "as", "by", "int", "long", "double", "decimal", "string", "boolean", "date", "timestamp",
    )
    val token = "[a-zA-Z_][a-zA-Z0-9_]*".r
    token.findAllMatchIn(sql).map(_.matched).filterNot(tok =>
      sqlKeywords.contains(tok.toLowerCase) || tok.matches("[0-9]+"))
      .toSeq.distinct
  }

  /** Build a calc [[Measure]] from the `calculated_measures:` section.
    *
    * The expression is parsed by [[CalcExpr]] so identifiers resolve through the
    * SemanticScope — this is what makes the framework's calc classification work.
    * `all(name)` references become `t.all(name)` for percent-of-total. */
  private def buildCalcMeasure(name: String, cfg: Any): Measure = {
    val (exprStr, description, _) = parseMetricConfig(cfg, "calculated measure", name)
    Measure(name, t => CalcExpr(t, exprStr), description)
  }

  // -------------------------------------------------------------------------
  // Join application
  // -------------------------------------------------------------------------

  /** Apply joins declared in YAML to a model. Joins are applied left-to-right
    * (chained), matching the Scala DSL fluent join API.
    *
    * After each join, the joined model's dimensions are re-added with an `alias.` prefix
    * so they are groupable by their aliased name (e.g. `carriers.name`) - mirroring what
    * a Scala consumer writes explicitly as Dimension("carriers.name", t => t("name")).
    *
    * Join keys must share the same column name on both sides (left_on == right_on),
    * matching semantica's equi-join engine. Asymmetric keys are a future enhancement. */
  private def applyJoins(
      model: SemanticTable,
      joins: Map[String, Map[String, Any]],
      allModels: Map[String, SemanticTable],
      modelName: String,
  ): SemanticTable = {
    var result = model
    joins.foreach { case (alias, jCfg) =>
      val joinModelName = jCfg.get("model") match {
        case Some(m: String) => m
        case _ => throw new IllegalArgumentException(
          s"Join '$alias' in model '$modelName' must specify a 'model' field.")
      }
      val right = allModels.getOrElse(joinModelName,
        throw new IllegalArgumentException(
          s"Join '$alias' references model '$joinModelName' which is not defined. " +
            s"Available: ${allModels.keys.toSeq.sorted.mkString(", ")}"))

      val joinType = jCfg.get("type").map(_.toString).getOrElse("one")
      joinType match {
        case "cross" =>
          result = result.join_cross(right)
        case "one" | "many" =>
          val leftOn = jCfg.get("left_on").map(_.toString).getOrElse(
            throw new IllegalArgumentException(
              s"Join '$alias' (type $joinType) must specify 'left_on'."))
          val rightOn = jCfg.get("right_on").map(_.toString).getOrElse(
            throw new IllegalArgumentException(
              s"Join '$alias' (type $joinType) must specify 'right_on'."))
          if (leftOn != rightOn)
            throw new IllegalArgumentException(
              s"Join '$alias': left_on ('$leftOn') and right_on ('$rightOn') must match. " +
                s"semantica's equi-join requires the same key column name on both tables. " +
                s"Rename the column in one table so both use '$leftOn'.")
          val on: (JoinSide, JoinSide) => org.apache.spark.sql.Column =
            (l, r) => l(leftOn) === r(rightOn)
          joinType match {
            case "one"  => result = result.join_one(right, on)
            case "many" => result = result.join_many(right, on)
          }
          // Re-expose the joined model's dimensions under the alias prefix so consumers
          // can groupBy("carriers.name") etc. The expr still references the bare column
          // (which survives the join), only the dimension NAME is prefixed.
          val rightDims = right.dimensions.values.toSeq.collect {
            case d if d.name != leftOn =>
              Dimension(s"$alias.${d.name}", d.expr, d.description)
          }
          if (rightDims.nonEmpty) result = result.withDimensions(rightDims: _*)
        case other =>
          throw new IllegalArgumentException(
            s"Join '$alias' has invalid type '$other'. Use: one, many, or cross.")
      }
    }
    result
  }

  // -------------------------------------------------------------------------

  /** Extract (expression, description, extras) from a metric config value.
    *
    * Supports two YAML formats:
    *   - Shorthand string: `total_distance: "sum(distance)"`
    *   - Full mapping:
    *       total_distance:
    *         expr: "sum(distance)"
    *         description: "..."
    */
  private def parseMetricConfig(
      cfg: Any,
      metricType: String,
      name: String,
  ): (String, Option[String], Map[String, Any]) = cfg match {
    case s: String =>
      (s, None, Map.empty)
    case m: Map[_, _] @unchecked =>
      val map = m.asInstanceOf[Map[String, Any]]
      val exprStr = map.get("expr") match {
        case Some(e: String) => e
        case _ => throw new IllegalArgumentException(
          s"$metricType '$name' must specify an 'expr' field (string).")
      }
      val description = map.get("description").collect { case d: String => d }
      val extras = map.filterNot { case (k, _) => k == "expr" || k == "description" }
      (exprStr, description, extras)
    case _ =>
      throw new IllegalArgumentException(
        s"$metricType '$name': expected a string or mapping, got ${cfg.getClass.getSimpleName}.")
  }

  /** Coerce a YAML value to `Map[String, Any]` where each value may be a string
    * (shorthand) or a mapping (full form). Used for dimensions/measures/calculated_measures. */
  private def asStringToAnyMap(v: Any, context: String): Map[String, Any] = v match {
    case m: Map[_, _] @unchecked =>
      m.asInstanceOf[Map[String, Any]]
    case _ =>
      throw new IllegalArgumentException(
        s"$context: expected a mapping, got ${v.getClass.getSimpleName}.")
  }

  /** Coerce a YAML value to `Map[String, Map[String, Any]]` with a context label for errors. */
  private def asStringMap(v: Any, context: String): Map[String, Map[String, Any]] = v match {
    case m: Map[_, _] @unchecked =>
      m.asInstanceOf[Map[String, Any]].map {
        case (k: String, inner: Map[_, _] @unchecked) =>
          k -> inner.asInstanceOf[Map[String, Any]]
        case (k, inner) =>
          throw new IllegalArgumentException(
            s"$context: key '$k' must be a mapping, got ${inner.getClass.getSimpleName}.")
      }
    case _ =>
      throw new IllegalArgumentException(
        s"$context: expected a mapping, got ${v.getClass.getSimpleName}.")
  }

  private def tableNotFound(name: String, available: Iterable[String]): IllegalArgumentException =
    new IllegalArgumentException(
      s"Table '$name' not found in the provided tables map. " +
        s"Available: ${available.toSeq.sorted.mkString(", ")}")
}
