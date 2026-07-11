package io.semantica.tools

import scala.jdk.CollectionConverters._
import java.io.File
import org.yaml.snakeyaml.Yaml


/** Generate browsable HTML documentation from semantica YAML model files.
  *
  * Run with:
  *   mvn scala:run -DmainClass=io.semantica.tools.Main \
  *     -Dexec.args="docsgen --path models/ --out docs/index.html"
  *
  * Reads one or more YAML files, parses the model definitions, and emits a
  * self-contained HTML page. No external dependencies — all CSS is embedded.
  */
class DocsGen {

  /** Generate HTML docs for one YAML file or a directory of YAML files. */
  def fromFile(path: String): String = {
    val f = new File(path)
    if (f.isDirectory) generateHtml(loadDir(path))
    else generateHtml(loadFile(path))
  }

  /** Write HTML content to a file. */
  def write(path: String, content: String): Unit = {
    val f = new File(path)
    f.getParentFile.mkdirs()
    val pw = new java.io.PrintWriter(f)
    try pw.write(content) finally pw.close()
  }

  // -------------------------------------------------------------------------
  // YAML parsing
  // -------------------------------------------------------------------------

  private def loadDir(dir: String): Seq[ModelEntry] = {
    val d = new File(dir)
    require(d.isDirectory, s"$dir is not a directory")
    val filter = new java.io.FileFilter {
      override def accept(f: File) =
        f.isFile && (f.getName.endsWith(".yml") || f.getName.endsWith(".yaml"))
    }
    d.listFiles(filter).toSeq.sortBy(_.getName).flatMap(f => loadFile(f.getPath))
  }

  private def loadFile(path: String): Seq[ModelEntry] = {
    val yaml = new Yaml()
    val raw = yaml.load[java.util.Map[String, java.util.Map[String, Any]]](
      scala.io.Source.fromFile(path).mkString)
    if (raw == null) return Nil
    raw.asScala.map { case (modelName, modelMap) =>
      ModelEntry(
        name        = modelName,
        table       = strOpt(modelMap, "table"),
        description = strOpt(modelMap, "description"),
        dimensions  = parseDims(modelMap),
        measures    = parseMeasures(modelMap),
        joins       = parseJoins(modelMap),
      )
    }.toSeq.sortBy(_.name)
  }

  private def parseDims(m: java.util.Map[String, Any]): Seq[DimEntry] = {
    val dims = m.get("dimensions")
    if (dims == null) return Nil
    dims.asInstanceOf[java.util.Map[String, Any]].asScala.toMap.map { case (name, v) =>
      val map = v match {
        case x: java.util.Map[String, Any] => x.asScala.toMap
        case _ => Map.empty[String, Any]
      }
      val meta = parseMeta(map.get("metadata"))
      DimEntry(
        name        = name,
        expr        = strOpt(map, "expr"),
        description = strOpt(map, "description"),
        isTimeDim   = map.get("is_time_dimension") == java.lang.Boolean.TRUE,
        isEntity    = map.get("is_entity") == java.lang.Boolean.TRUE ||
                      meta.get("tags").contains("identifier"),
        metadata    = meta,
      )
    }.toSeq.sortBy(_.name)
  }

  private def parseMeasures(m: java.util.Map[String, Any]): Seq[MeasEntry] = {
    val meas = m.get("measures")
    if (meas == null) return Nil
    meas.asInstanceOf[java.util.Map[String, Any]].asScala.toMap.map { case (name, v) =>
      val map = v match {
        case s: String          => Map("expr" -> s)
        case x: java.util.Map[String, Any] => x.asScala.toMap
        case _ => Map.empty[String, Any]
      }
      val meta = parseMeta(map.get("metadata"))
      MeasEntry(
        name        = name,
        expr        = strOpt(map, "expr"),
        description = strOpt(map, "description"),
        unit        = meta.get("unit"),
        metadata    = meta,
      )
    }.toSeq.sortBy(_.name)
  }

  private def parseJoins(m: java.util.Map[String, Any]): Seq[JoinEntry] = {
    val joins = m.get("joins")
    if (joins == null) return Nil
    joins.asInstanceOf[java.util.Map[String, Any]].asScala.toMap.map { case (alias, v) =>
      val map = v match {
        case x: java.util.Map[String, Any] => x.asScala.toMap
        case _ => Map.empty[String, Any]
      }
      JoinEntry(
        alias      = alias,
        model      = strOpt(map, "model"),
        joinType   = strOpt(map, "type"),
        leftOn     = strOpt(map, "left_on"),
        rightOn    = strOpt(map, "right_on"),
        description = strOpt(map, "description"),
      )
    }.toSeq.sortBy(_.alias)
  }

  private def parseMeta(v: Any): Map[String, String] =
    if (v == null) Map.empty
    else v match {
      case m: java.util.Map[String, Any] => m.asScala.toMap.mapValues(_.toString).toMap
      case _ => Map.empty
    }

  private def strOpt(m: Map[String, Any], key: String): Option[String] =
    m.get(key).map(_.toString.trim).filter(_.nonEmpty)

  private def strOpt(m: java.util.Map[String, Any], key: String): Option[String] =
    Option(m.get(key)).map(_.toString.trim).filter(_.nonEmpty)

  // -------------------------------------------------------------------------
  // HTML generation
  // -------------------------------------------------------------------------

  private def generateHtml(models: Seq[ModelEntry]): String = {
    val ts = java.time.Instant.now().toString.take(10)
    val nav = models.map(m => s"""<a href="#m-${slug(m.name)}">${escHtml(m.name)}</a>""").mkString("\n  ")
    val body = models.map(renderModel).mkString("\n")

    s"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Semantica Model Catalog</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
         background:#f8f9fa; color:#24292e; line-height:1.6; }
  .wrap { display:flex; min-height:100vh; }
  nav { width:240px; background:#1c2128; color:#e6edf3; padding:24px 0;
        position:sticky; top:0; height:100vh; overflow-y:auto; flex-shrink:0; }
  nav h1 { font-size:13px; font-weight:700; padding:0 20px 20px;
            border-bottom:1px solid #30363d; margin-bottom:12px; letter-spacing:.02em; }
  nav a { display:block; padding:5px 20px; color:#8b949e; text-decoration:none; font-size:13px; }
  nav a:hover { color:#e6edf3; background:#30363d; }
  nav .sep { padding:16px 20px 6px; font-size:10px; font-weight:700;
              text-transform:uppercase; color:#6e7681; letter-spacing:.08em; }
  main { flex:1; padding:40px 48px; max-width:960px; }
  .card { background:#fff; border:1px solid #d0d7de; border-radius:8px;
           padding:32px; margin-bottom:32px; }
  .card h1 { font-size:26px; font-weight:700; margin-bottom:4px; }
  .card .src { font-size:13px; color:#57606a; margin-bottom:16px; }
  .card .desc { font-size:14px; color:#57606a; margin-bottom:24px; }
  .card h2 { font-size:16px; font-weight:700; margin:32px 0 12px;
              padding-bottom:8px; border-bottom:1px solid #eaecef; }
  .card h2:first-of-type { margin-top:0; }
  .card h3 { font-size:14px; font-weight:600; margin:20px 0 6px; }
  .badge { display:inline-block; padding:1px 8px; border-radius:12px;
            font-size:11px; font-weight:500; margin-left:6px; }
  .badge-time  { background:#dafbe1; color:#1a7f37; }
  .badge-entity{ background:#dbeafe; color:#1d4ed8; }
  .badge-pii   { background:#fee2e2; color:#991b1b; }
  table { width:100%; border-collapse:collapse; font-size:13px; margin:8px 0 24px; }
  th { text-align:left; padding:8px 12px; background:#f6f8fa; border:1px solid #d0d7de;
       font-size:12px; font-weight:600; white-space:nowrap; }
  td { padding:8px 12px; border:1px solid #d0d7de; vertical-align:top; }
  td.name { font-family:monospace; font-size:12px; color:#0550ae; white-space:nowrap; }
  td code { font-size:11px; background:#f6f8fa; padding:1px 5px; border-radius:4px; }
  .expr { font-size:11px; color:#6e7781; font-family:monospace; margin-top:2px; }
  .desc { font-size:13px; color:#57606a; margin-top:4px; }
  .join-row td { background:#f6f8fa; }
  .tag { font-size:11px; padding:1px 6px; border-radius:4px; background:#d0d7de; color:#57606a;
          margin-right:4px; }
  .breadcrumb { font-size:12px; color:#8c959f; margin-bottom:8px; }
  .breadcrumb a { color:#58a6ff; text-decoration:none; }
  footer { padding:24px 48px; border-top:1px solid #d0d7de;
            color:#8c959f; font-size:12px; }
  @media(max-width:768px) {
    .wrap { flex-direction:column; }
    nav { width:100%; height:auto; position:static; }
    main { padding:24px 20px; }
  }
</style>
</head>
<body>
<div class="wrap">
<nav>
  <h1>📊 Semantica</h1>
  <div class="sep">Models</div>
  $nav
</nav>
<main>
$body
</main>
</div>
<footer>Generated by semantica docsgen · $ts</footer>
</body>
</html>"""
  }

  private def renderModel(m: ModelEntry): String = {
    val dimCount  = m.dimensions.size
    val measCount = m.measures.size
    val joinCount = m.joins.size
    val stats = Seq(
      m.table.map(_ => "base model"),
      Option.when(dimCount > 0)(s"$dimCount dims"),
      Option.when(measCount > 0)(s"$measCount measures"),
      Option.when(joinCount > 0)(s"$joinCount joins"),
    ).flatten.mkString(" · ")

    val dimBadge = if (m.dimensions.exists(_.isEntity))
      s""" <span class="badge badge-entity">${m.dimensions.count(_.isEntity)} entity</span>"""
    else ""
    val timeBadge = if (m.dimensions.exists(_.isTimeDim))
      s""" <span class="badge badge-time">${m.dimensions.count(_.isTimeDim)} time</span>"""
    else ""
    val piiBadge = if (m.dimensions.exists(d => d.metadata.get("pii").contains("true")))
      s""" <span class="badge badge-pii">${m.dimensions.count(d => d.metadata.get("pii").contains("true"))} pii</span>"""
    else ""

    s"""<div class="card" id="m-${slug(m.name)}">
  <div class="breadcrumb"><a href="#">↑ All models</a></div>
  <h1>${escHtml(m.name)}</h1>
  ${m.table.map(t => s"""<div class="src">table: <code>${escHtml(t)}</code></div>""").getOrElse("")}
  ${m.description.map(d => s"""<div class="desc">${escHtml(d)}</div>""").getOrElse("")}
  <div class="desc" style="margin-bottom:20px">$stats</div>

  <h2>Dimensions$dimBadge$timeBadge$piiBadge</h2>
  ${if (m.dimensions.isEmpty) "<p style='color:#8c959f;font-size:13px'>No dimensions defined.</p>"
    else renderDimTable(m.dimensions)}

  <h2>Measures${if (measCount > 0) s""" <span style="color:#57606a;font-weight:400;font-size:13px">$measCount</span>""" else ""}</h2>
  ${if (m.measures.isEmpty) "<p style='color:#8c959f;font-size:13px'>No measures defined.</p>"
    else renderMeasTable(m.measures)}

  ${if (m.joins.nonEmpty) s"""
  <h2>Joins <span style="color:#57606a;font-weight:400;font-size:13px">$joinCount</span></h2>
  ${renderJoinTable(m.joins)}""" else ""}
</div>"""
  }

  private def renderDimTable(dims: Seq[DimEntry]): String = {
    val rows = dims.map { d =>
      val badges = Seq(
        Option.when(d.isTimeDim)(s"""<span class="badge badge-time">time</span>"""),
        Option.when(d.isEntity)(s"""<span class="badge badge-entity">entity</span>"""),
        Option.when(d.metadata.get("pii").contains("true"))(s"""<span class="badge badge-pii">pii</span>"""),
      ).flatten.mkString(" ")
      val tags = d.metadata.get("tags").map(_.split("[,\\[\\]]").filter(_.nonEmpty)
        .map(t => s"""<span class="tag">${escHtml(t.trim)}</span>""").mkString(" ")).getOrElse("")
      val expr = d.expr.map(e => s"""<div class="expr">${escHtml(e)}</div>""").getOrElse("")
      s"""<tr>
  <td class="name">${escHtml(d.name)}<br>$expr</td>
  <td>${d.description.map(escHtml).getOrElse("")}</td>
  <td>$badges$tags</td>
</tr>"""
    }.mkString("\n")
    s"""<table>
  <thead><tr><th style="width:220px">Name</th><th>Description</th><th>Flags</th></tr></thead>
  <tbody>$rows</tbody>
</table>"""
  }

  private def renderMeasTable(measures: Seq[MeasEntry]): String = {
    val rows = measures.map { m =>
      val tags = m.metadata.get("aggregation").map(a => s"""<span class="tag">${escHtml(a)}</span>""").getOrElse("")
      s"""<tr>
  <td class="name">${escHtml(m.name)}</td>
  <td>${m.expr.map(e => s"""<code>${escHtml(e)}</code>""").getOrElse("")}</td>
  <td>${m.description.map(escHtml).getOrElse("")}</td>
  <td>${escHtml(m.unit.getOrElse(""))}</td>
</tr>"""
    }.mkString("\n")
    s"""<table>
  <thead><tr><th style="width:200px">Name</th><th style="width:220px">Expr</th><th>Description</th><th>Unit</th></tr></thead>
  <tbody>$rows</tbody>
</table>"""
  }

  private def renderJoinTable(joins: Seq[JoinEntry]): String = {
    val rows = joins.map { j =>
      val on = Seq(j.leftOn, j.rightOn).flatten.map(escHtml).mkString(" = ")
      s"""<tr class="join-row">
  <td class="name">${escHtml(j.alias)}</td>
  <td>${j.model.map(escHtml).getOrElse("—")}</td>
  <td>${j.joinType.map(escHtml).getOrElse("—")}</td>
  <td><code>$on</code></td>
</tr>"""
    }.mkString("\n")
    s"""<table>
  <thead><tr><th style="width:160px">Alias</th><th>Model</th><th>Type</th><th>On</th></tr></thead>
  <tbody>$rows</tbody>
</table>"""
  }

  private def slug(name: String) = name.replaceAll("[^a-zA-Z0-9_-]", "-").toLowerCase

  private def escHtml(s: String): String =
    s.replaceAll("&", "&amp;")
     .replaceAll("<", "&lt;")
     .replaceAll(">", "&gt;")
     .replaceAll("\"", "&quot;")

  // -------------------------------------------------------------------------
  // Data models
  // -------------------------------------------------------------------------

  private case class ModelEntry(
    name: String,
    table: Option[String],
    description: Option[String],
    dimensions: Seq[DimEntry],
    measures: Seq[MeasEntry],
    joins: Seq[JoinEntry],
  )

  private case class DimEntry(
    name: String,
    expr: Option[String],
    description: Option[String],
    isTimeDim: Boolean,
    isEntity: Boolean,
    metadata: Map[String, String],
  )

  private case class MeasEntry(
    name: String,
    expr: Option[String],
    description: Option[String],
    unit: Option[String],
    metadata: Map[String, String],
  )

  private case class JoinEntry(
    alias: String,
    model: Option[String],
    joinType: Option[String],
    leftOn: Option[String],
    rightOn: Option[String],
    description: Option[String],
  )
}
