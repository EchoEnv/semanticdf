package io.semanticdf

import org.apache.spark.sql.Row

import scala.reflect.macros.blackbox

/** Macro implementation for [[ResultDecoder.derive]].
  *
  * Kept in a separate object so the macro plumbing (`c.universe`,
  * `c.Expr`, error reporting) doesn't pollute the public surface
  * of [[ResultDecoder]] or its companion.
  *
  * == Generation strategy ==
  *
  * For a case class `Foo(a: String, b: Long)` the macro emits:
  * {{{
  *   new ResultDecoder[Foo] {
  *     def decode(row: Row): Foo = new Foo(row.getString(0), row.getLong(1))
  *   }
  * }}}
  *
  * Field-type \u2192 Row-getter mapping is centralized in [[rowGetFor]]. Each
  * supported primitive type maps to the corresponding `Row.getX(i)`. An
  * unsupported field type produces a compile-time error pointing at the
  * constructor parameter; the user can supply a manual decoder instead.
  *
  * == Why blackbox ==
  *
  * The macro is `blackbox` (returns `Expr[T]` without exposing the type
  * universe to the call site) because the call site only needs a real
  * `ResultDecoder[T]` \u2014 not the macro machinery. The macro machinery is
  * private to this file; users see only the `def derive[T] = macro ...`
  * declaration.
  *
  * == Why no paradise / macro-compat plugin ==
  *
  * `def foo = macro fooImpl` is core Scala 2.13. No annotation macros
  * (`@deriveDecoder`), no compiler plugin \u2014 just the built-in `def macro`
  * mechanism. Annotation macros would require the `paradise` plugin and
  * are not necessary here.
  */
object ResultDecoderMacros {

  /** Scala 2 blackbox macro implementation of [[ResultDecoder.derive]]. */
  def deriveImpl[T: c.WeakTypeTag](c: blackbox.Context): c.Expr[ResultDecoder[T]] = {
    import c.universe._

    val tpe = weakTypeOf[T]

    // (1) Reject non-case-classes up front \u2014 the macro only knows how to
    //     call the synthetic `apply` constructor of a case class. Sealed
    //     traits, abstract classes, regular classes, etc. have no `apply`
    //     we can call with positional arguments alone.
    if (!tpe.typeSymbol.isClass ||
        !tpe.typeSymbol.asClass.isCaseClass) {
      c.abort(
        c.enclosingPosition,
        s"ResultDecoder.derive requires a case class; got `$tpe`. " +
        s"For primitives, use the built-in instances (ResultDecoder[String], " +
        s"etc.). For sealed traits or non-case-class shapes, supply a manual " +
        s"`ResultDecoder` instance via `implicit val`.",
      )
    }

    // (2) Locate the primary constructor. For case classes the compiler
    //     emits a primary `apply` on the companion that we can call with
    //     positional args. We walk to the case-class primary constructor
    //     directly (rather than the synthetic apply) to stay robust to
    //     custom companions.
    val ctor = tpe.decl(termNames.CONSTRUCTOR)
    if (!ctor.isMethod || !ctor.asMethod.isPrimaryConstructor) {
      c.abort(c.enclosingPosition,
        s"ResultDecoder.derive could not locate the primary constructor for `$tpe`.")
    }

    // Case-class constructors have exactly one param list by spec; we
    // assume single-list. If a future case class adds an auxiliary param
    // list, extend the loop below.
    val params = ctor.asMethod.paramLists.headOption.getOrElse(Nil)

    // (3) For each constructor parameter, emit `row.getX(i)` where `X`
    //     matches the parameter's static type. Unknown field types abort
    //     the compilation with the parameter name in the error so the
    //     user can fix it (rename, hand-write decoder, etc.).
    val rowReads: Seq[Tree] = params.zipWithIndex.map { case (sym, idx) =>
      val fieldTpe = sym.typeSignature
      rowGetFor(c)(tpe, sym, fieldTpe, idx)
    }

    // (4) Emit `new Foo(row.getString(0), row.getLong(1), ...)`. We invoke
    //     the primary constructor directly so this works even when the user
    //     has shadowed `Foo.apply` with a custom companion. For an empty
    //     parameter list we emit `new Foo()`; otherwise we splice each
    //     `row.getX(i)` tree into the constructor call by name.
    val newInstance: Tree =
      if (rowReads.isEmpty)
        q"new $tpe()"
      else
        q"new $tpe(..$rowReads)"

    // Wrap the generated instance in a fresh `ResultDecoder[T]` and lift
    // to an Expr. The Expr is then inlined at the call site so the
    // user pays no runtime overhead beyond the manual case.
    //
    // The decode parameter is named via `c.freshName` so it never clashes
    // with any local the call site might be using. Inside the body we
    // bind `val row = $decodeArgName` so the spliced `row.getX(i)` reads
    // resolve to the parameter without us needing to subst every spliced
    // `row.Ident` to the fresh name (which is fragile across compilers).
    val decodeArgName = TermName(c.freshName("row"))
    c.Expr[ResultDecoder[T]](
      q"""
        new _root_.io.semanticdf.ResultDecoder[$tpe] {
          def decode($decodeArgName: _root_.org.apache.spark.sql.Row): $tpe = {
            val row: _root_.org.apache.spark.sql.Row = $decodeArgName
            $newInstance
          }
        }
      """,
    )
  }

  /** Map a field type to the corresponding `Row.getX(i)` call. Aborts with
    * a clear, parameter-named error if the type isn't in the supported set.
    *
    * Add new supported types here in two steps: (a) the type pattern, (b)
    * the `row.getX` method name. Keep the two lists in sync.
    *
    * @param ownerTpe The case-class type that owns this constructor param.
    *                  Used only for error messages.
    */
  private def rowGetFor(c: blackbox.Context)
      (ownerTpe: c.Type, paramSym: c.Symbol, fieldTpe: c.Type, idx: Int): c.Tree = {
    import c.universe._

    fieldTpe match {
      case t if t <:< typeOf[String]                   => q"row.getString($idx)"
      case t if t <:< typeOf[Int]                      => q"row.getInt($idx)"
      case t if t <:< typeOf[Long]                     => q"row.getLong($idx)"
      case t if t <:< typeOf[Double]                   => q"row.getDouble($idx)"
      case t if t <:< typeOf[Float]                    => q"row.getFloat($idx)"
      case t if t <:< typeOf[Boolean]                  => q"row.getBoolean($idx)"
      case t if t <:< typeOf[Short]                    => q"row.getShort($idx)"
      case t if t <:< typeOf[Byte]                     => q"row.getByte($idx)"
      case t if t <:< typeOf[java.math.BigDecimal]     => q"row.getDecimal($idx)"
      case other =>
        c.abort(
          c.enclosingPosition,
          s"ResultDecoder.derive does not support field type `$fieldTpe` " +
          s"(parameter `${paramSym.name.toString.trim}` of `$ownerTpe`). " +
          s"Supported: String, Int, Long, Double, Float, Boolean, Short, " +
          s"Byte, java.math.BigDecimal. Supply a manual `ResultDecoder` " +
          s"instance via `implicit val` for richer shapes.",
        )
    }
  }
}
