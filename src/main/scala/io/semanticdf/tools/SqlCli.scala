package io.semanticdf.tools

import io.semanticdf.{Predicate, SemanticTable, SortKey}

/** Minimal SQL adapter for the `semanticdf query` CLI.
  *
  * Covers the 80% case — see [[parse]] for the grammar.
  *
  * Implementation: tokenizer + small recursive-descent over the token
  * stream. No external SQL parser. */
object SqlCli {

  final case class SelItem(name: String, alias: Option[String])
  final case class Parsed(
      model:    String,
      selItems: Seq[SelItem],
      where:    Option[Predicate],
      orderBy:  Seq[SortKey],
      limit:    Option[Int],
  )
  final case class Resolved(
      model:      String,
      dimensions: Seq[String],
      measures:   Seq[String],
      where:      Option[Predicate],
      orderBy:    Seq[SortKey],
      limit:      Option[Int],
  )

  /** Parse a SQL string. Grammar:
    *
    *   query   := SELECT sel FROM model [WHERE cond] [ORDER BY ord] [LIMIT n]
    *   sel     := item (',' item)*
    *   item    := '*' | ident ['AS' ident]
    *   cond    := atom (('AND'|'OR') atom)*
    *   atom    := ident op value | '(' cond ')'
    *   op      := '=' | '!=' | '<' | '<=' | '>' | '>='
    *   value   := number | "'string'"
    *   ord     := ident ['ASC'|'DESC'] (',' ord)*
    *
    * Tokens are case-insensitive for keywords, case-sensitive for identifiers.
    * String literals use SQL single-quote convention; `''` inside means a literal `'`.
    */
  def parse(sql: String): Parsed = {
    // Strip the query into chunks at major clause boundaries. Keeps each
    // clause's parsing independent and small.
    val tokens = tokenize(sql)
    val p = new Parser(tokens)
    p.parseQuery()
  }

  /** Resolve a Parsed query against a known model. */
  def resolve(parsed: Parsed, model: SemanticTable): Resolved = {
    val measureNames = model.measures.keySet
    val dimNames     = model.dimensions.keySet
    val dims   = scala.collection.mutable.ListBuffer.empty[String]
    val meas   = scala.collection.mutable.ListBuffer.empty[String]
    parsed.selItems.foreach { item =>
      if (item.name == "*") {
        dimNames.foreach(dims += _)
        model.measures.keys.foreach(meas += _)
      } else if (measureNames.contains(item.name)) meas += item.name
      else if (dimNames.contains(item.name)) dims += item.name
      else throw new IllegalArgumentException(
        s"SELECT references unknown field '${item.name}'. " +
        s"Dims: ${dimNames.toSeq.sorted.mkString(", ")}. " +
        s"Measures: ${measureNames.toSeq.sorted.mkString(", ")}.")
    }
    Resolved(parsed.model, dims.toSeq, meas.toSeq, parsed.where, parsed.orderBy, parsed.limit)
  }

  /** Apply a Parsed query against a model. */
  def apply(parsed: Parsed, model: SemanticTable): SemanticTable = {
    val r = resolve(parsed, model)
    model.query(
      measures   = r.measures,
      dimensions = r.dimensions,
      where      = r.where,
      orderBy    = r.orderBy,
      limit      = r.limit,
    )
  }

  // ── Tokenizer ────────────────────────────────────────────────────
  private sealed trait T
  private case class Kw(s: String) extends T
  private case class Id(s: String) extends T
  private case class Num(s: String) extends T
  private case class Str(s: String) extends T
  private case class Op(s: String) extends T
  private case class Punc(s: String) extends T

  private val KWORDS = Set(
    "SELECT", "FROM", "WHERE", "AND", "OR",
    "ORDER", "BY", "ASC", "DESC", "LIMIT", "AS", "GROUP",
  )

  private def tokenize(sql: String): Vector[T] = {
    val out = Vector.newBuilder[T]
    var i = 0
    while (i < sql.length) {
      val c = sql.charAt(i)
      if (c.isWhitespace) i += 1
      else if (c == '\'') {
        val start = i; i += 1
        val buf = new StringBuilder("'")
        while (i < sql.length && sql.charAt(i) != '\'') {
          if (sql.charAt(i) == '\'' && i + 1 < sql.length && sql.charAt(i + 1) == '\'') {
            buf.append("''"); i += 2
          } else { buf.append(sql.charAt(i)); i += 1 }
        }
        if (i >= sql.length) throw new IllegalArgumentException(
          s"SQL parse failed at column $start: unterminated string literal")
        buf.append('\'')
        i += 1
        out += Str(buf.toString)
      } else if (c.isLetter || c == '_') {
        val start = i
        while (i < sql.length && (sql.charAt(i).isLetterOrDigit || sql.charAt(i) == '_')) i += 1
        val word = sql.substring(start, i)
        val upper = word.toUpperCase
        if (KWORDS.contains(upper)) out += Kw(upper) else out += Id(word)
      } else if (c.isDigit) {
        val start = i
        while (i < sql.length && sql.charAt(i).isDigit) i += 1
        if (i < sql.length && sql.charAt(i) == '.') {
          i += 1
          while (i < sql.length && sql.charAt(i).isDigit) i += 1
        }
        out += Num(sql.substring(start, i))
      } else if (c == '*' || c == ',' || c == '(' || c == ')') {
        out += Punc(c.toString); i += 1
      } else if ("=<>!".contains(c)) {
        val two = if (i + 1 < sql.length) sql.substring(i, i + 2) else ""
        if (two == "<=" || two == ">=" || two == "!=") {
          out += Op(two); i += 2
        } else { out += Op(c.toString); i += 1 }
      } else {
        throw new IllegalArgumentException(
          s"SQL parse failed at column $i: unexpected character '$c'")
      }
    }
    out.result()
  }

  // ── Parser ──────────────────────────────────────────────────────
  private class Parser(val tokens: Vector[T]) {
    private var pos = 0

    private def here: Int = pos
    private def peek: Option[T] = if (pos < tokens.length) Some(tokens(pos)) else None
    private def eatKw(kw: String): Unit = {
      peek match {
        case Some(Kw(`kw`)) => pos += 1
        case Some(other) =>
          throw new IllegalArgumentException(
            s"SQL parse failed at column $here: expected '$kw', got '${tokenName(other)}'")
        case None =>
          throw new IllegalArgumentException(
            s"SQL parse failed at column $here: expected '$kw', got <end of input>")
      }
    }
    private def eatPunc(p: String): Unit = {
      peek match {
        case Some(Punc(`p`)) => pos += 1
        case Some(other) =>
          throw new IllegalArgumentException(
            s"SQL parse failed at column $here: expected '$p', got '${tokenName(other)}'")
        case None =>
          throw new IllegalArgumentException(
            s"SQL parse failed at column $here: expected '$p', got <end of input>")
      }
    }
    private def tokenName(t: T): String = t match {
      case Kw(s)  => s
      case Id(s)  => s
      case Num(s) => s
      case Str(s) => s
      case Op(s)  => s
      case Punc(s) => s
    }

    private def expectId(what: String): String = peek match {
      case Some(Id(s)) => pos += 1; s
      case Some(other) =>
        throw new IllegalArgumentException(
          s"SQL parse failed at column $here: expected $what, got '${tokenName(other)}'")
      case None =>
        throw new IllegalArgumentException(
          s"SQL parse failed at column $here: expected $what, got <end of input>")
    }

    def parseQuery(): Parsed = {
      eatKw("SELECT")
      val selItems = parseSelList()
      eatKw("FROM")
      val model = expectId("model name")
      consumeGroupBy()
      val where = if (peek.contains(Kw("WHERE"))) { pos += 1; Some(parseCond()) } else None
      consumeGroupBy()
      val orderBy = if (peek.contains(Kw("ORDER"))) {
        pos += 1; eatKw("BY"); parseOrderList()
      } else Seq.empty
      consumeGroupBy()
      val limit = if (peek.contains(Kw("LIMIT"))) {
        pos += 1
        peek match {
          case Some(Num(s)) => pos += 1; Some(s.toInt)
          case Some(other) =>
            throw new IllegalArgumentException(
              s"SQL parse failed at column $here: expected number after LIMIT, got '${tokenName(other)}'")
          case None =>
            throw new IllegalArgumentException(
              s"SQL parse failed at column $here: expected number after LIMIT")
        }
      } else None
      consumeGroupBy()
      if (pos < tokens.length)
        throw new IllegalArgumentException(
          s"SQL parse failed at column $here: unexpected '${tokenName(tokens(pos))}'")
      Parsed(model, selItems, where, orderBy, limit)
    }

    /** Accept (and ignore) GROUP BY — the model decides grouping based on
      * the SELECT items, so the explicit GROUP BY column list is advisory.
      * We tolerate it in any clause position (between FROM-AND, AND-WHERE,
      * POST-WHERE, post-ORDER, post-LIMIT) so this parser is permissive
      * about query shape. */
    private def consumeGroupBy(): Unit = {
      if (peek.contains(Kw("GROUP"))) {
        pos += 1; eatKw("BY")
        while (peek.exists { case Id(_) => true; case _ => false }) pos += 1
      }
    }

    private def parseSelList(): Seq[SelItem] = {
      val first = parseSelItem()
      val rest = scala.collection.mutable.ListBuffer.empty[SelItem]
      while (peek.contains(Punc(","))) {
        pos += 1
        rest += parseSelItem()
      }
      first +: rest.toSeq
    }

    private def parseSelItem(): SelItem = peek match {
      case Some(Punc("*")) => pos += 1; SelItem("*", None)
      case Some(Id(_)) =>
        val name = expectId("identifier")
        val alias = if (peek.contains(Kw("AS"))) {
          pos += 1; Some(expectId("alias"))
        } else None
        SelItem(name, alias)
      case Some(other) =>
        throw new IllegalArgumentException(
          s"SQL parse failed at column $here: expected '*' or identifier, got '${tokenName(other)}'")
      case None =>
        throw new IllegalArgumentException(
          s"SQL parse failed at column $here: expected '*' or identifier, got <end of input>")
    }

    private def parseCond(): Predicate = {
      var left = parseAtom()
      while (peek.exists {
        case Kw("AND") | Kw("OR") => true
        case _ => false
      }) {
        val isAnd = peek.contains(Kw("AND"))
        pos += 1
        val right = parseAtom()
        left = if (isAnd) Predicate.And(left, right) else Predicate.Or(left, right)
      }
      left
    }

    private def parseAtom(): Predicate = {
      if (peek.contains(Punc("("))) {
        pos += 1
        val inner = parseCond()
        eatPunc(")")
        return inner
      }
      val field = expectId("field name")
      val opStr = peek match {
        case Some(Op(s)) => pos += 1; s
        case Some(other) =>
          throw new IllegalArgumentException(
            s"SQL parse failed at column $here: expected comparison operator, got '${tokenName(other)}'")
        case None =>
          throw new IllegalArgumentException(
            s"SQL parse failed at column $here: expected comparison operator, got <end of input>")
      }
      val value: Any = peek match {
        case Some(Num(s)) =>
          pos += 1
          if (s.contains('.')) s.toDouble else s.toLong
        case Some(Str(s)) =>
          pos += 1
          // Strip outer single quotes, unescape ''
          s.substring(1, s.length - 1).replace("''", "'")
        case Some(other) =>
          throw new IllegalArgumentException(
            s"SQL parse failed at column $here: expected value, got '${tokenName(other)}'")
        case None =>
          throw new IllegalArgumentException(
            s"SQL parse failed at column $here: expected value, got <end of input>")
      }
      Predicate.Compare(opToName(opStr), field, value)
    }

    /** Map tokenizer operator strings to Predicate.Compare names.
      * The Predicate factory only accepts "eq" / "ne" / "lt" / "le" / "gt" / "ge";
      * SQL uses "=" / "!=" / "<" / "<=" / ">" / ">=". */
    private def opToName(op: String): String = op match {
      case "="  => "eq"
      case "!=" => "ne"
      case "<"  => "lt"
      case "<=" => "le"
      case ">"  => "gt"
      case ">=" => "ge"
      case other => throw new IllegalArgumentException(s"Unknown compare op: $other")
    }

    private def parseOrderList(): Seq[SortKey] = {
      val first = parseOrderItem()
      val rest = scala.collection.mutable.ListBuffer.empty[SortKey]
      while (peek.contains(Punc(","))) {
        pos += 1
        rest += parseOrderItem()
      }
      first +: rest.toSeq
    }

    private def parseOrderItem(): SortKey = {
      val name = expectId("sort column")
      val asc  = !peek.contains(Kw("DESC"))
      if (peek.exists { case Kw("ASC") | Kw("DESC") => true; case _ => false }) pos += 1
      if (asc) SortKey.Asc(name) else SortKey.Desc(name)
    }
  }
}
