package io.semanticdf.tools

import io.semanticdf.lineage.{JoinLineage, ModelLineage, WorkspaceLineage}

/** DOT-graph rendering for a [[WorkspaceLineage]].
  *
  * Renders one node per model + edges for `upstreamModels`. Aimed at
  * `graphviz dot` (or any DOT renderer). No external deps — hand-written
  * because the DOT format is small.
  *
  * Node IDs are the modelIds (sanitized to DOT-safe identifiers). The
  * join info is shown on the edge label as `"L.key=R.key, cardinality"`.
  */
private[tools] object LineageCli {

  /** Render a DOT graph for a [[WorkspaceLineage]]. Returns the raw DOT
    * source — pipe to `dot -Tsvg` for an image, or print as-is. */
  def toDot(wl: WorkspaceLineage): String = {
    val sb = new StringBuilder
    sb.append("digraph semanticdf_lineage {\n")
    sb.append("  rankdir=LR;\n")
    sb.append("  node [shape=box, style=rounded, fontname=\"Helvetica\"];\n")
    sb.append("  edge [fontname=\"Helvetica\", fontsize=10];\n\n")

    // Nodes — one per model. Label includes source table + model name.
    val ids = scala.collection.mutable.LinkedHashMap[String, String]()
    wl.models.values.toSeq.sortBy(_.modelName).foreach { m =>
      val id = safeId(m.modelId)
      ids(m.modelId) = id
      val label = escapeLabel(
        s"${m.modelName}\\n" +
        s"[${m.sourceKind.toString.toLowerCase}" +
        m.sourceTable.fold("")(t => s": $t") +
        s"]\\n" +
        s"${m.dimensions.length} dims, ${m.measures.length} measures" +
        (if (m.joins.nonEmpty) s", ${m.joins.length} joins" else "")
      )
      sb.append(s"""  "$id" [label="$label"];\n""")
    }

    // Edges — one per upstreamModels relation. If the join has
    // explicit keys + cardinality, attach as edge label.
    sb.append("\n")
    wl.models.values.toSeq.foreach { m =>
      m.upstreamModels.foreach { upId =>
        val fromId = ids.getOrElse(upId, safeId(upId))
        val toId   = ids.getOrElse(m.modelId, safeId(m.modelId))
        // Find the corresponding join (best-effort: match by upstream modelId)
        val join  = m.joins.find { j =>
          j.leftModel == upId || j.rightModel == upId
        }
        val label = join.fold("")(j => edgeLabel(j))
        val labelAttr = if (label.nonEmpty) s""" [label="$label"]""" else ""
        sb.append(s"""  "$fromId" -> "$toId"$labelAttr;\n""")
      }
    }

    sb.append("}\n")
    sb.toString
  }

  /** Make a DOT-safe node identifier from a modelId (which may contain
    * `.` or other characters that DOT would parse as syntax). */
  private def safeId(s: String): String =
    s.replaceAll("[^A-Za-z0-9_]", "_")

  /** Escape a label for use in DOT — backslashes and double-quotes. */
  private def escapeLabel(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"")

  /** Format a `JoinLineage` as the edge label. */
  private def edgeLabel(j: JoinLineage): String = {
    val keys = if (j.keys.isEmpty) ""
               else j.keys.map { case (l, r) => s"$l=$r" }.mkString(", ")
    val card = s"[${j.cardinality}]"
    if (keys.isEmpty) card else s"$card $keys"
  }
}
