package io.semanticdf.core.model

import io.semanticdf.core.expr.Calculator

/** Engine-portable model-validator — Phase 2 contract.
  *
  * A single validation pass invoked exactly once by [[Model.of]],
  * with the order:
  *   (1) name is non-blank
  *   (2) no duplicate dimension / measure / calc-measure names
  *   (3) every calc-measure refers to a declared measure
  *   (4) calc DAG is acyclic + depth <= `maxDepthBound`
  *   (5) extension envelope fits limits
  *   (6) policy defaults are well-formed (typed refs resolve later)
  *
  * Per scala-data-driven-refactor §2 ("shape/validity separate"):
  * validity is enforced exactly once, at the boundary. After
  * `Model.of` succeeds, every downstream function trusts the type.
  * Re-validating deep in business logic is a smell.
  *
  * ==Why a private object (vs. inline in `Model.of`)==
  *
  * `Model.of` is the smart constructor — its job is to build the
  * `Model`. The validation logic is a SEPARATE concern, extracted
  * for testability and clarity. The validator is a `private[model]`
  * object so callers must go through `Model.of` (the boundary).
  *
  * ==Why core (engine-portable)==
  *
  * The validation rules are universal — every engine that builds
  * portable models needs the same shape checks. The depth cap
  * (`maxDepthBound`) is engine-specific (passed by the caller).
  */
private[model] object ModelValidator {

  /** The single validation pass.
    *
    * @return `Right(())` if the model is well-formed;
    *         `Left(ModelValidationError)` with the FIRST violation
    *         found. (Per the design: the first violation is
    *         reported; the model is either fully valid or
    *         invalid — there's no "partially valid" state.) */
  def validate(
      name:              String,
      source:            SourceRef,
      dimensions:        List[Dimension],
      measures:          List[Measure],
      calculatedMeasures: List[CalculatedMeasure],
      joins:             List[JoinSpec],
      filters:           List[FilterSpec],
      rollups:           List[RollupSpec],
      extensions:        Map[String, ExtensionValue],
      defaultPolicies:   ModelPolicyDefaults,
      maxDepthBound:     Int = Int.MaxValue,
  ): Either[ModelValidationError, Unit] = {
    // (1) name is non-blank
    val trimmed = Option(name).map(_.trim).getOrElse("")
    if (trimmed.isEmpty)
      Left(ModelValidationError.InvalidName("name is blank"))
    else {
      val declaredMeasures:   Set[String] = measures.iterator.map(_.name).toSet
      val declaredDimensions: Set[String] = dimensions.iterator.map(_.name).toSet
      val declaredCalc:       Set[String] = calculatedMeasures.iterator.map(_.name).toSet

      // (2a) duplicate dimension/measure names (must be disjoint sets)
      if (declaredMeasures.exists(declaredDimensions.contains))
        Left(ModelValidationError.DuplicateMember(
          "dimension/measure",
          declaredMeasures.iterator.filter(declaredDimensions.contains).next(),
        ))
      // (2b) duplicate calc-measure names
      else if (calculatedMeasures.size != declaredCalc.size)
        Left(ModelValidationError.DuplicateMember(
          "calculatedMeasure",
          calculatedMeasures.iterator.map(_.name)
            .foldLeft((Set.empty[String], Option.empty[String])) {
              case ((seen, found), n) =>
                if (found.isDefined) (seen, found)
                else if (seen.contains(n)) (seen, Some(n))
                else (seen + n, found)
            }._2.get,
        ))
      // (2c) calc-measure name must not collide with a declared dimension or measure
      else if (declaredCalc.exists(n =>
          declaredDimensions.contains(n) || declaredMeasures.contains(n)))
        Left(ModelValidationError.DuplicateMember(
          "calculatedMeasure",
          declaredCalc.iterator
            .filter(n => declaredDimensions.contains(n) || declaredMeasures.contains(n))
            .next(),
        ))
      // (3) every calc-measure refers to a declared measure (base or calc)
      else {
        val declaredMembers: Set[String] = declaredMeasures ++ declaredCalc
        val unresolvedRef = calculatedMeasures.iterator.flatMap { cm =>
          Calculator.measureNamesOf(cm.expr).iterator.filterNot(declaredMembers.contains)
        }.toList
        if (unresolvedRef.nonEmpty)
          Left(ModelValidationError.UnknownReference(
            "calculatedMeasures", unresolvedRef.head,
          ))
        // (4) cycle + depth
        else {
          CalcGraph.checkAcyclicAndDepth(
            declaredCalc, calculatedMeasures, maxDepthBound,
          ).left.map(ModelValidationError.CalcDepthExceeded(_, maxDepthBound))
            .flatMap(_ =>
              // (5) extension envelope fits limits
              ExtensionLimits.check(extensions)
                .left.map(e => ModelValidationError.ExtensionEnvelopeExceeded(
                  e.fieldCount, e.byteCount,
                ))
                .map(_ => Right(()))
                .flatten
              // (6) policy defaults are well-formed by construction
              // (the underlying ADTs are closed) — no check needed
            )
        }
      }
    }
  }
}