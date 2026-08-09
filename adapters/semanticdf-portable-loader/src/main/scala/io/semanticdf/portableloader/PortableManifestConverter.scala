package io.semanticdf.portableloader

import io.semanticdf.core.expr.Expr
import io.semanticdf.core.field.TimeGrain
import io.semanticdf.core.manifest.{
  ManifestError,
  PortableCalculatedMeasure,
  PortableDimension,
  PortableFilter,        // referenced for the filter-deferred limitation
  PortableJoin,
  PortableMeasure,
  PortableModel,
  PortableRollup,
  PortableSource,
  PortableStatus,
}
import io.semanticdf.core.model.{
  CalculatedMeasure,
  Dimension,
  JoinSpec,
  Measure,
  Model,
  ModelStatus,
  ProviderRef,
  RollupFreshnessSpec,
  RollupMeasureSpec,
  RollupSpec,
  SourceRef,
}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn, JoinKind}

/** Converter: `PortableModel` → `core.Model`.
  *
  * Per the v0.3.2 design doc (PR #437): the reader (3A.2) reads
  * YAML → `PortableModel`, then this converter produces a
  * validated `core.Model` via the existing `Model.of` smart
  * constructor.
  *
  * Lives in `adapters/semanticdf-portable-loader/` (alongside
  * `YamlManifestLoader`) because it's logically part of the
  * loader pipeline. Users see ONE public API
  * (`PortableManifestLoader.load(path)`) that does read + convert
  * internally.
  *
  * ==Known limitations (v1)==
  *
  *   1. **Dimension expr simplification**: `PortableDimension.expr`
  *      is assumed to be a column name (FieldRef). Dimensions whose
  *      expression aliases a different column are silently treated
  *      as the literal name. Same limitation as `ModelBridge.toModel`
  *      (PR #408).
  *
  *   2. **Measure aggregate detection**: `PortableMeasure.kind` is
  *      a String. We map it to `AggregateFn` via a small dispatcher.
  *      Unknown kinds fall back to `AggregateFn.Sum` (matches
  *      `ModelBridge.toModel` placeholder behavior).
  *
  *   3. **Join kind mapping**: `PortableJoin.kind` is one of
  *      `"one" | "many" | "cross"` (matches legacy convention).
  *      Mapped to `JoinKind.Inner | Inner | Cross`. The mapping is
  *      conservative — `"one"` and `"many"` both become `Inner`.
  *
  *   4. **Filter conversion deferred**: `PortableFilter.expr` is
  *      not converted. The reader surfaces this as a typed error
  *      (`FilterConversionUnsupported`). See the design doc for the
  *      v0.3.3 plan.
  *
  *   5. **Rollup grain normalization**: `PortableRollup.grain` is
  *      normalized via `TimeGrain.normalize` (existing engine-
  *      portable function). Unknown grains throw
  *      `IllegalArgumentException` (existing `TimeGrain.normalize`
  *      behavior) — caught and converted to a typed
  *      `ManifestError` here.
  *
  *   6. **Calculated measure expr simplification**: same as #1
  *      for `PortableCalculatedMeasure.expr`.
  *
  *   7. **No policy extraction**: `ModelPolicyDefaults.none` (no
  *      way to infer policies from portable YAML in v1).
  *
  * ==Standard compliance==
  *
  * Per docs/design/error-handling-style.md: returns `Either[ManifestError, Model]`.
  * No `Either[String, _]`. Specific `IllegalArgumentException` (from
  * `TimeGrain.normalize`) is caught at the IO boundary (the
  * `normalize` call IS the IO boundary for grain parsing) and
  * converted to a typed `ManifestError` immediately.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Engine-portable. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-portable-loader/src/main/scala/` */
object PortableManifestConverter {

  /** Convert a `PortableModel` to a validated `core.Model`. */
  def toModel(pm: PortableModel): Either[ManifestError, Model] = {
    for {
      source     <- convertSource(pm.source).right
      dimensions <- convertDimensions(pm.dimensions).right
      measures   <- convertMeasures(pm.measures).right
      calcMeas   <- convertCalculatedMeasures(pm.calculatedMeasures).right
      joins      <- convertJoins(pm.joins).right
      rollups    <- convertRollups(pm.rollups, pm.name).right
      status     <- convertStatus(pm.status).right
      model <- Model.of(
        name               = pm.name,
        source             = source,
        dimensions         = dimensions,
        measures           = measures,
        calculatedMeasures = calcMeas,
        joins              = joins,
        filters            = Nil,  // Deferred: see FilterConversionUnsupported
        rollups            = rollups,
        extensions         = pm.extensions,
        description        = pm.description,
        version            = pm.version,
        status             = status,
      ).left.map(err => ManifestError.DomainValidation(
        reason    = validationErrorMessage(err),
        modelName = pm.name,
      ))
    } yield model
  }

  /** Map a `ModelValidationError` to a stable message string.
    *
    * Per scala-data-driven-refacer §3 ("default: sealed trait +
    * match"): exhaustive pattern match on the 6 ModelValidationError
    * cases. */
  private def validationErrorMessage(err: io.semanticdf.core.model.ModelValidationError): String = err match {
    case io.semanticdf.core.model.ModelValidationError.InvalidName(reason) =>
      s"invalid name: $reason"
    case io.semanticdf.core.model.ModelValidationError.DuplicateMember(kind, name) =>
      s"duplicate $kind: $name"
    case io.semanticdf.core.model.ModelValidationError.UnknownReference(referent, target) =>
      s"unknown reference: $referent → $target"
    case io.semanticdf.core.model.ModelValidationError.CalcDepthExceeded(depth, max) =>
      s"calc depth exceeded: $depth > $max"
    case io.semanticdf.core.model.ModelValidationError.ExtensionEnvelopeExceeded(fieldCount, byteCount) =>
      s"extension envelope exceeded: $fieldCount fields, $byteCount bytes"
    case io.semanticdf.core.model.ModelValidationError.FilterConversionUnsupported(reason) =>
      s"filter conversion unsupported: $reason"
  }

  // -- Source --

  private def convertSource(s: PortableSource): Either[ManifestError, SourceRef] = {
    s match {
      case PortableSource.ByName(catalog, namespace, table) =>
        Right(SourceRef.ByName(
          catalog = catalog, namespace = namespace, table = table
        ))
      case PortableSource.ByPath(path, format, options) =>
        Right(SourceRef.ByPath(
          path = path, format = format, options = options
        ))
      case PortableSource.ByProvider(provider, identifier) =>
        // Map to `ProviderRef.TableResolver(name = provider)` (the
        // closest match for portable's (provider, identifier) tuple).
        // Future work can add a richer mapping (DataFrameSource,
        // etc.) if needed.
        Right(SourceRef.ByProvider(
          provider = ProviderRef.TableResolver(name = provider)
        ))
    }
  }

  // -- Dimensions --

  private def convertDimensions(pds: List[PortableDimension]): Either[ManifestError, List[Dimension]] = {
    val dims = pds.map { pd =>
      Dimension(
        name = pd.name,
        expr = Expr.FieldRef(name = pd.expr),  // simplification per limitation #1
      )
    }
    Right(dims)
  }

  // -- Measures --

  private def convertMeasures(pms: List[PortableMeasure]): Either[ManifestError, List[Measure]] = {
    val meas = pms.map { pm =>
      val fn = parseAggregateFn(pm.kind)
      Measure(
        name = pm.name,
        expr = AggregateCall(
          fn    = fn,
          input = Some(Expr.FieldRef(name = pm.expr)),  // assume column-name reference
          alias = pm.name,
        ),
      )
    }
    Right(meas)
  }

  /** Parse the legacy `kind` string into an `AggregateFn`.
    * Unknown kinds default to `Sum` (matches `ModelBridge.toModel`
    * placeholder behavior — limitation #2). */
  private def parseAggregateFn(kind: Option[String]): AggregateFn = {
    kind.map(_.toLowerCase).map {
      case "sum"           => AggregateFn.Sum
      case "count"         => AggregateFn.Count
      case "countdistinct" => AggregateFn.CountDistinct
      case "avg" | "mean"  => AggregateFn.Avg
      case "min"           => AggregateFn.Min
      case "max"           => AggregateFn.Max
      case "stddev"        => AggregateFn.StddevSample
      case "stddev_pop"    => AggregateFn.StddevPopulation
      case "variance"      => AggregateFn.VarianceSample
      case "variance_pop"  => AggregateFn.VariancePopulation
      case "median"        => AggregateFn.Median
      case _               => AggregateFn.Sum  // fallback per limitation #2
    }.getOrElse(AggregateFn.Sum)  // missing kind → Sum
  }

  // -- Calculated Measures --

  private def convertCalculatedMeasures(pcms: List[PortableCalculatedMeasure]): Either[ManifestError, List[CalculatedMeasure]] = {
    val cms = pcms.map { pcm =>
      CalculatedMeasure(
        name = pcm.name,
        expr = Expr.FieldRef(name = pcm.expr),  // simplification per limitation #6
      )
    }
    Right(cms)
  }

  // -- Joins --

  private def convertJoins(pjs: List[PortableJoin]): Either[ManifestError, List[JoinSpec]] = {
    val js = pjs.map { pj =>
      JoinSpec(
        name       = pj.name,
        rightModel = pj.rightSource,
        kind       = parseJoinKind(pj.kind),
        keys       = pj.keys.map(k => (k, k)),  // self-join convention (limitation)
      )
    }
    Right(js)
  }

  /** Map the legacy `kind` string to `JoinKind`. Per limitation #3:
    * `"one"` and `"many"` both map to `Inner` (conservative). */
  private def parseJoinKind(kind: String): JoinKind = {
    kind.toLowerCase match {
      case "one" | "many" | "inner" => JoinKind.Inner
      case "left"                  => JoinKind.Left
      case "right"                 => JoinKind.Right
      case "full" | "outer"        => JoinKind.Full
      case "cross"                 => JoinKind.Cross
      case _                       => JoinKind.Inner  // fallback to conservative
    }
  }

  // -- Rollups --

  private def convertRollups(prs: List[PortableRollup], modelName: String): Either[ManifestError, List[RollupSpec]] = {
    prs.foldLeft[Either[ManifestError, List[RollupSpec]]](Right(Nil)) { (acc, pr) =>
      acc.flatMap { specs =>
        val grain = try {
          TimeGrain.normalize(pr.grain)
        } catch {
          // Per the IO-boundary rule: catch SPECIFIC exception types
          // and convert to typed error at the boundary.
          case _: IllegalArgumentException =>
            return Left(ManifestError.InvalidEnumValue(
              field   = "rollups.grain",
              value   = pr.grain,
              allowed = TimeGrain.Order.toSet,
            ))
        }
        val measureSpecs = pr.measures.map { m =>
          RollupMeasureSpec(
            name       = m,
            aggregator = AggregateFn.Sum,  // default per limitation
            storageCol = m,
          )
        }
        Right(specs :+ RollupSpec(
          name       = pr.name,
          baseModel  = modelName,
          dimensions = Nil,  // grain is the only dimension (limitation)
          measures   = measureSpecs,
          freshness  = RollupFreshnessSpec.NoTracking,  // v1 default
        ))
      }
    }
  }

  // -- Status --

  private def convertStatus(ps: PortableStatus): Either[ManifestError, ModelStatus] = {
    ps match {
      case PortableStatus.Draft      => Right(ModelStatus.Draft)
      case PortableStatus.Published  => Right(ModelStatus.Published)
      case PortableStatus.Deprecated => Right(ModelStatus.Deprecated)
    }
  }
}
