package io.semanticdf.core.model

/** Engine-portable model container — Phase 2 contract.
  * Mirrors the design doc §4.4.1 "Model".
  *
  * The [[Model]] is the central portable model container — the
  * engine-portable shape that flows through the v2 manifest, the
  * MCP wire format, and every engine adapter's compile step.
  *
  * ==Why a `final class` (not a case class)==
  *
  * The `final class` (with `val` fields, `extends Serializable`)
  * matches the design's pattern: "private[model] final class Model"
  * with the smart constructor `Model.of` returning
  * `Either[ModelValidationError, Model]`. The class has explicit
  * `val` accessors (not case-class accessors) so callers can't
  * accidentally bypass the smart constructor.
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior lives
  * elsewhere"): the `Model` has NO methods — it's pure data. All
  * operations (validation, rendering, compile) live elsewhere
  * (`ModelValidator`, the manifest writer, the engine adapter).
  *
  * ==Why a smart constructor `Model.of`==
  *
  * Per scala-data-driven-refactor §2 ("shape/validity separate"):
  * validity is enforced exactly once, at the boundary. `Model.of`
  * runs `ModelValidator.validate(...)` once; on success, returns
  * the immutable `Model`; on failure, returns
  * `Left(ModelValidationError)`. The caller pattern-matches to
  * decide what to do.
  *
  * ==Why `private[model] def unsafe`==
  *
  * Trusted internal callers (the v1 reader, the manifest writer)
  * can construct a `Model` WITHOUT validation, via `Model.unsafe`.
  * This is for migration paths where the caller has already
  * validated the data via another path. Production callers MUST
  * use `Model.of`.
  *
  * ==Why every field is `val`==
  *
  * Immutability is critical for the portable model. Per
  * scala-data-driven-refactor §1, the model flows through Spark
  * shuffles, distributed cache, the v2 manifest writer, and the
  * MCP wire format. Any `var` field is a runtime
  * `NotSerializableException` waiting to happen.
  *
  * ==Why core (engine-portable)==
  *
  * The model SHAPE (name + description + source + dimensions +
  * measures + ...) is universal across engines. The COMPILE
  * (transforming this model into an engine-specific plan) is in
  * the engine adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: `final class` with `val` fields (no behavior)
  * - `extends Serializable` for Java-serialization round-trip
  * - No `Any` fields, no closures, no DataFrame refs
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/Model.scala`
  */
final class Model private[model] (
    val name:              String,
    val description:       Option[String],
    val source:            SourceRef,
    val dimensions:        List[Dimension],
    val measures:          List[Measure],
    val calculatedMeasures: List[CalculatedMeasure],
    val joins:             List[JoinSpec],
    val filters:           List[FilterSpec],
    val version:           Int,
    val status:            ModelStatus,
    val rollups:           List[RollupSpec],
    val defaultPolicies:   ModelPolicyDefaults,
    val extensions:        Map[String, ExtensionValue],
) extends Serializable

object Model {

  /** The canonical smart constructor. Validates the model once at
    * the boundary, then returns the immutable model.
    *
    * @param maxDepthBound the engine-specific calc-depth cap
    *                      (`Int.MaxValue` for SQL engines, tighter
    *                      for the spark adapter)
    * @return `Right(Model)` if valid, `Left(ModelValidationError)`
    *         with the first violation found. */
  def of(
      name:               String,
      source:             SourceRef,
      dimensions:         List[Dimension],
      measures:           List[Measure],
      calculatedMeasures: List[CalculatedMeasure]   = Nil,
      joins:              List[JoinSpec]            = Nil,
      filters:            List[FilterSpec]          = Nil,
      rollups:            List[RollupSpec]          = Nil,
      defaultPolicies:    ModelPolicyDefaults       = ModelPolicyDefaults.none,
      extensions:         Map[String, ExtensionValue] = Map.empty,
      description:        Option[String]            = None,
      version:            Int                       = 1,
      status:             ModelStatus               = ModelStatus.Draft,
      maxDepthBound:      Int                       = Int.MaxValue,
  ): Either[ModelValidationError, Model] =
    ModelValidator.validate(
      name = name, source = source, dimensions = dimensions,
      measures = measures, calculatedMeasures = calculatedMeasures,
      joins = joins, filters = filters, rollups = rollups,
      extensions = extensions, defaultPolicies = defaultPolicies,
      maxDepthBound = maxDepthBound,
    ).map { _ =>
      new Model(
        name = name, description = description, source = source,
        dimensions = dimensions, measures = measures,
        calculatedMeasures = calculatedMeasures, joins = joins,
        filters = filters, version = version, status = status,
        rollups = rollups, defaultPolicies = defaultPolicies,
        extensions = extensions,
      )
    }

  /** Lightweight constructor (no validation) for trusted internal
    * callers (e.g. the v1 reader after migration). Production
    * callers MUST use `Model.of`. */
  private[model] def unsafe(
      name:              String,
      description:       Option[String],
      source:            SourceRef,
      dimensions:        List[Dimension],
      measures:          List[Measure],
      calculatedMeasures: List[CalculatedMeasure],
      joins:             List[JoinSpec],
      filters:           List[FilterSpec],
      version:           Int,
      status:            ModelStatus,
      rollups:           List[RollupSpec],
      defaultPolicies:   ModelPolicyDefaults,
      extensions:        Map[String, ExtensionValue],
  ): Model =
    new Model(
      name = name, description = description, source = source,
      dimensions = dimensions, measures = measures,
      calculatedMeasures = calculatedMeasures, joins = joins,
      filters = filters, version = version, status = status,
      rollups = rollups, defaultPolicies = defaultPolicies,
      extensions = extensions,
    )
}