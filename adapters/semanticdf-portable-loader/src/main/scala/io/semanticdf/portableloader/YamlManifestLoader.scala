package io.semanticdf.portableloader

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

import io.semanticdf.core.manifest.{
  ManifestError,
  PortableCalculatedMeasure,
  PortableDimension,
  PortableFilter,
  PortableJoin,
  PortableMeasure,
  PortableModel,
  PortableRollup,
  PortableSource,
  PortableStatus,
}

/** YAML manifest loader — reads portable YAML into a `PortableModel`.
  *
  * Per the v0.3.2 design doc (PR #437): this is the YAML-reader
  * intermediate. Step 3 PR #A.2 of the Manifest → `core.Model`
  * migration.
  *
  * Lives in `adapters/semanticdf-portable-loader/` (NOT in
  * `semanticdf-core`) because it has IO dependencies (Jackson YAML).
  * Per the user's architectural intent: `semanticdf-core` stays
  * Jackson/YAML-free (pure data shapes only); IO concerns live in
  * adapter modules.
  *
  * ==Why JsonNode walking (vs. direct Jackson case-class binding)==
  *
  * Jackson's polymorphic deserialization (sealed trait + discriminator)
  * requires `@JsonTypeInfo` annotations on the type. Adding Jackson
  * annotations to `core.manifest.PortableSource` would violate
  * `semanticdf-core`'s "Jackson-free" design intent.
  *
  * Instead, we parse the YAML to a `JsonNode` tree and walk it
  * manually. This:
  *   1. Keeps `semanticdf-core` truly Jackson-free (no annotations
  *      to leak the dependency)
  *   2. Makes the YAML format explicit (the walking code IS the
  *      format spec — easy to read and audit)
  *   3. Gives precise error messages (we control the path tracking)
  *
  * ==Why accumulate errors (vs. fail-fast)==
  *
  * Per scala-error-handling "fail loud, but show all problems":
  * the loader accumulates ALL parse errors during one pass and
  * surfaces them. This lets users fix all problems in one edit
  * pass instead of playing whack-a-mole.
  *
  * ==Standard compliance==
  *
  * Per docs/design/error-handling-style.md: public API returns
  * `Either[ManifestError, PortableModel]`. No `Either[String, _]`.
  * Catch SPECIFIC exception types at the IO boundary (Jackson's
  * `JsonProcessingException`); convert to typed `ManifestError`
  * immediately. No catch-all.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. The loader is engine-portable.
  * Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-portable-loader/src/main/scala/` */
object YamlManifestLoader {

  /** The Jackson ObjectMapper configured for YAML.
    * Reused across loads (it's thread-safe per Jackson docs). */
  private val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())

  /** Load a portable manifest from a YAML file. */
  def load(path: Path): Either[ManifestError, PortableModel] = {
    if (!Files.exists(path)) {
      return Left(ManifestError.MissingField(field = "file", path = Nil))
    }
    try {
      val bytes = Files.readAllBytes(path)
      val text  = new String(bytes, "UTF-8")
      loadString(text)
    } catch {
      case e: java.io.IOException =>
        Left(ManifestError.YamlSyntaxError(
          reason = s"could not read file: ${e.getMessage}"
        ))
    }
  }

  /** Load a portable manifest from a YAML string. */
  def loadString(yaml: String): Either[ManifestError, PortableModel] = {
    try {
      val root = mapper.readTree(yaml)
      parseRoot(root)
    } catch {
      case e: com.fasterxml.jackson.core.JsonProcessingException =>
        Left(ManifestError.YamlSyntaxError(
          reason = Option(e.getOriginalMessage).getOrElse(e.getMessage),
          path   = parseLocationPath(e.getLocation),
        ))
      case e: java.io.IOException =>
        Left(ManifestError.YamlSyntaxError(reason = s"I/O error: ${e.getMessage}"))
    }
  }

  // -- Top-level parsing --

  private def parseRoot(root: JsonNode): Either[ManifestError, PortableModel] = {
    val errors = scala.collection.mutable.ListBuffer.empty[ManifestError]

    val sourceOpt = readSource(root.get("source"), errors)
    val descOpt   = readOptionalString(root, "description")
    val versionOpt = readInt(root, "version", default = 1, errors)

    val dimensions         = readList(root.get("dimensions"),       errors)(parseDimension)
    val measures           = readList(root.get("measures"),         errors)(parseMeasure)
    val calculatedMeasures = readList(root.get("calculatedMeasures"), errors)(parseCalculatedMeasure)
    val joins              = readList(root.get("joins"),            errors)(parseJoin)
    val filters            = readList(root.get("filters"),          errors)(parseFilter)
    val rollups            = readList(root.get("rollups"),          errors)(parseRollup)

    val statusOpt = readOptionalString(root, "status").flatMap { s =>
      PortableStatus.parse(s) match {
        case Some(parsed) => Some(parsed)
        case None        =>
          errors += ManifestError.InvalidEnumValue(
            field   = "status",
            value   = s,
            allowed = Set("draft", "published", "deprecated"),
          )
          None
      }
    }

    // Filter conversion deferred (limitation #4) — surface typed error
    // if filters are present so users know to migrate the predicates.
    if (filters.nonEmpty) {
      errors += ManifestError.FilterConversionUnsupported(
        filterCount = filters.size
      )
    }

    sourceOpt match {
      case None =>
        // Source missing — already added to errors. Return them.
        Left(errors.toList match {
          case Nil      => ManifestError.MissingField(field = "source")
          case one :: Nil => one
          case multiple  => ManifestError.Multiple(multiple)
        })
      case Some(source) =>
        val model = PortableModel(
          name               = readString(root, "name", errors).getOrElse(""),
          description        = descOpt,
          source             = source,
          dimensions         = dimensions,
          measures           = measures,
          calculatedMeasures = calculatedMeasures,
          joins              = joins,
          filters            = filters,
          rollups            = rollups,
          version            = versionOpt,
          status             = statusOpt.getOrElse(PortableStatus.Draft),
        )
        validateAndReturn(model, errors.toList)
    }
  }

  private def validateAndReturn(model: PortableModel, accumulated: List[ManifestError]): Either[ManifestError, PortableModel] = {
    val errors = scala.collection.mutable.ListBuffer.from(accumulated)

    // `name` must be non-empty
    if (model.name == null || model.name.trim.isEmpty) {
      errors += ManifestError.MissingField(field = "name")
    }
    // `source` must be valid (already validated by parseSource)

    errors.toList match {
      case Nil      => Right(model)
      case one :: Nil => Left(one)
      case multiple  => Left(ManifestError.Multiple(multiple))
    }
  }

  // -- Generic node-reading helpers --

  private def hasField(node: JsonNode, field: String): Boolean =
    node != null && node.has(field) && !node.get(field).isNull

  private def readString(node: JsonNode, field: String, errors: scala.collection.mutable.ListBuffer[ManifestError]): Option[String] = {
    if (node == null || !node.has(field)) None
    else {
      val child = node.get(field)
      if (child.isNull) None
      else if (!child.isTextual) {
        errors += ManifestError.TypeMismatch(field = field, expected = "String", actual = child.getNodeType.toString)
        None
      } else Some(child.asText())
    }
  }

  private def readOptionalString(node: JsonNode, field: String): Option[String] = {
    if (node == null || !node.has(field) || node.get(field).isNull) None
    else if (!node.get(field).isTextual) None
    else Some(node.get(field).asText())
  }

  private def readInt(node: JsonNode, field: String, default: Int, errors: scala.collection.mutable.ListBuffer[ManifestError]): Int = {
    if (node == null || !node.has(field) || node.get(field).isNull) default
    else {
      val child = node.get(field)
      if (!child.isInt) {
        errors += ManifestError.TypeMismatch(field = field, expected = "Int", actual = child.getNodeType.toString)
        default
      } else child.asInt()
    }
  }

  private def readList[A](
      node:    JsonNode,
      errors:  scala.collection.mutable.ListBuffer[ManifestError]
  )(parser: JsonNode => Either[ManifestError, A]): List[A] = {
    if (node == null || node.isNull) Nil
    else if (!node.isArray) {
      errors += ManifestError.TypeMismatch(
        field    = node.asText(),  // not meaningful but gives a marker
        expected = "array",
        actual   = node.getNodeType.toString,
      )
      Nil
    } else {
      node.asScala.toList.zipWithIndex.flatMap { case (item, idx) =>
        parser(item) match {
          case Right(a) => Some(a)
          case Left(err) =>
            errors += err  // already has path info; surface as-is
            None
        }
      }
    }
  }

  // -- Source parsing (sealed trait → walk discriminator) --

  private def readSource(node: JsonNode, errors: scala.collection.mutable.ListBuffer[ManifestError]): Option[PortableSource] = {
    if (node == null || node.isNull) {
      errors += ManifestError.MissingField(field = "source")
      return None
    }
    if (!node.isObject) {
      errors += ManifestError.TypeMismatch(field = "source", expected = "object", actual = node.getNodeType.toString)
      return None
    }
    val typeField = node.get("type")
    if (typeField == null || typeField.isNull) {
      errors += ManifestError.MissingField(field = "source.type")
      return None
    }
    val typeName = typeField.asText()
    typeName match {
      case "ByName" => parseByName(node, errors)
      case "ByPath" => parseByPath(node, errors)
      case "ByProvider" => parseByProvider(node, errors)
      case other =>
        errors += ManifestError.InvalidEnumValue(
          field   = "source.type",
          value   = other,
          allowed = Set("ByName", "ByPath", "ByProvider"),
        )
        None
    }
  }

  private def parseByName(node: JsonNode, errors: scala.collection.mutable.ListBuffer[ManifestError]): Option[PortableSource] = {
    val tableOpt = readString(node, "table", errors)
    tableOpt match {
      case None =>
        errors += ManifestError.InvalidValue(field = "source.table", reason = "must not be empty")
        None
      case Some(table) =>
        Some(PortableSource.ByName(
          catalog   = readOptionalString(node, "catalog"),
          namespace = readOptionalString(node, "namespace"),
          table     = table,
        ))
    }
  }

  private def parseByPath(node: JsonNode, errors: scala.collection.mutable.ListBuffer[ManifestError]): Option[PortableSource] = {
    val pathOpt   = readString(node, "path", errors)
    val formatOpt = readString(node, "format", errors)
    (pathOpt, formatOpt) match {
      case (Some(path), Some(format)) =>
        Some(PortableSource.ByPath(
          path   = path,
          format = format,
          options = parseOptions(node.get("options")),
        ))
      case _ =>
        if (pathOpt.isEmpty) errors += ManifestError.InvalidValue(field = "source.path", reason = "must not be empty")
        if (formatOpt.isEmpty) errors += ManifestError.InvalidValue(field = "source.format", reason = "must not be empty")
        None
    }
  }

  private def parseByProvider(node: JsonNode, errors: scala.collection.mutable.ListBuffer[ManifestError]): Option[PortableSource] = {
    val providerOpt = readString(node, "provider", errors)
    providerOpt match {
      case None =>
        errors += ManifestError.InvalidValue(field = "source.provider", reason = "must not be empty")
        None
      case Some(provider) =>
        Some(PortableSource.ByProvider(
          provider   = provider,
          identifier = readOptionalString(node, "identifier").getOrElse(""),
        ))
    }
  }

  private def parseOptions(node: JsonNode): Map[String, String] = {
    if (node == null || node.isNull || !node.isObject) Map.empty
    else {
      val fields = node.fields().asScala.toList
      fields.map(e => e.getKey -> e.getValue.asText()).toMap
    }
  }

  // -- Field parsers --

  private def parseDimension(node: JsonNode): Either[ManifestError, PortableDimension] = {
    val name = Option(node.get("name")).filter(!_.isNull).map(_.asText()).getOrElse("")
    val expr = Option(node.get("expr")).filter(!_.isNull).map(_.asText()).getOrElse("")
    if (name.isEmpty) Left(ManifestError.MissingField(field = "dimensions[].name"))
    else if (expr.isEmpty) Left(ManifestError.MissingField(field = "dimensions[].expr"))
    else Right(PortableDimension(name = name, expr = expr))
  }

  private def parseMeasure(node: JsonNode): Either[ManifestError, PortableMeasure] = {
    val name = Option(node.get("name")).filter(!_.isNull).map(_.asText()).getOrElse("")
    val expr = Option(node.get("expr")).filter(!_.isNull).map(_.asText()).getOrElse("")
    if (name.isEmpty) Left(ManifestError.MissingField(field = "measures[].name"))
    else if (expr.isEmpty) Left(ManifestError.MissingField(field = "measures[].expr"))
    else Right(PortableMeasure(
      name = name,
      expr = expr,
      kind = Option(node.get("kind")).filter(!_.isNull).map(_.asText()),
    ))
  }

  private def parseCalculatedMeasure(node: JsonNode): Either[ManifestError, PortableCalculatedMeasure] = {
    val name = Option(node.get("name")).filter(!_.isNull).map(_.asText()).getOrElse("")
    val expr = Option(node.get("expr")).filter(!_.isNull).map(_.asText()).getOrElse("")
    if (name.isEmpty) Left(ManifestError.MissingField(field = "calculatedMeasures[].name"))
    else if (expr.isEmpty) Left(ManifestError.MissingField(field = "calculatedMeasures[].expr"))
    else Right(PortableCalculatedMeasure(name = name, expr = expr))
  }

  private def parseJoin(node: JsonNode): Either[ManifestError, PortableJoin] = {
    val name        = Option(node.get("name")).filter(!_.isNull).map(_.asText()).getOrElse("")
    val kind        = Option(node.get("kind")).filter(!_.isNull).map(_.asText()).getOrElse("inner")
    val leftSource  = Option(node.get("leftSource")).filter(!_.isNull).map(_.asText()).getOrElse("")
    val rightSource = Option(node.get("rightSource")).filter(!_.isNull).map(_.asText()).getOrElse("")
    val keys = Option(node.get("keys")).filter(!_.isNull) match {
      case Some(k) if k.isArray => k.asScala.toList.map(_.asText())
      case _                    => Nil
    }
    if (name.isEmpty) Left(ManifestError.MissingField(field = "joins[].name"))
    else if (leftSource.isEmpty) Left(ManifestError.MissingField(field = "joins[].leftSource"))
    else if (rightSource.isEmpty) Left(ManifestError.MissingField(field = "joins[].rightSource"))
    else Right(PortableJoin(
      name        = name,
      kind        = kind,
      leftSource  = leftSource,
      rightSource = rightSource,
      keys        = keys,
    ))
  }

  private def parseFilter(node: JsonNode): Either[ManifestError, PortableFilter] = {
    val name = Option(node.get("name")).filter(!_.isNull).map(_.asText()).getOrElse("")
    val expr = Option(node.get("expr")).filter(!_.isNull).map(_.asText()).getOrElse("")
    if (name.isEmpty) Left(ManifestError.MissingField(field = "filters[].name"))
    else if (expr.isEmpty) Left(ManifestError.MissingField(field = "filters[].expr"))
    else Right(PortableFilter(name = name, expr = expr))
  }

  private def parseRollup(node: JsonNode): Either[ManifestError, PortableRollup] = {
    val name  = Option(node.get("name")).filter(!_.isNull).map(_.asText()).getOrElse("")
    val grain = Option(node.get("grain")).filter(!_.isNull).map(_.asText()).getOrElse("")
    val measures = Option(node.get("measures")).filter(!_.isNull) match {
      case Some(m) if m.isArray => m.asScala.toList.map(_.asText())
      case _                    => Nil
    }
    if (name.isEmpty) Left(ManifestError.MissingField(field = "rollups[].name"))
    else if (grain.isEmpty) Left(ManifestError.MissingField(field = "rollups[].grain"))
    else Right(PortableRollup(name = name, grain = grain, measures = measures))
  }

  /** Parse Jackson's location into our `List[String]` form. */
  private def parseLocationPath(loc: com.fasterxml.jackson.core.JsonLocation): List[String] = {
    if (loc == null) Nil
    else List(s"line ${loc.getLineNr}", s"column ${loc.getColumnNr}")
  }
}
