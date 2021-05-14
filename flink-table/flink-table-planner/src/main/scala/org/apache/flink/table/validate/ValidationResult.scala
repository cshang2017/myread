
package org.apache.flink.table.validate

/**
  * Represents the result of a validation.
  */
sealed trait ValidationResult {
  def isFailure: Boolean = !isSuccess
  def isSuccess: Boolean

  /**
    * Allows constructing a cascade of validation results.
    * The first failure result will be returned.
    */
  def orElse(other: ValidationResult): ValidationResult = {
    if (isSuccess) {
      other
    } else {
      this
    }
  }
}

/**
  * Represents the successful result of a validation.
  */
object ValidationSuccess extends ValidationResult {
  val isSuccess: Boolean = true
}

/**
  * Represents the failing result of a validation,
  * with a error message to show the reason of failure.
  */
case class ValidationFailure(message: String) extends ValidationResult {
  val isSuccess: Boolean = false
}
