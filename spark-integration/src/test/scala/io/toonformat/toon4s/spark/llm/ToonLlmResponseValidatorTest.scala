package io.toonformat.toon4s.spark.llm

import munit.FunSuite

class ToonLlmResponseValidatorTest extends FunSuite {

  test("validate: accepts normal response") {
    val result = ToonLlmResponseValidator.validate("Top users are Alice and Bob.")
    assert(result.valid)
    assert(result.issues.isEmpty)
  }

  test("validate: flags empty response") {
    val result = ToonLlmResponseValidator.validate("   ")
    assert(!result.valid)
    assert(result.issues.exists(_.isInstanceOf[ToonLlmResponseValidator.ValidationIssue.EmptyResponse]))
  }

  test("validate: flags format confusion response") {
    val result =
      ToonLlmResponseValidator.validate("I do not understand TOON. Please provide JSON.")
    assert(!result.valid)
    assert(result.hasConfusion)
  }

  test("validate: flags raw json echo") {
    val result = ToonLlmResponseValidator.validate("""{"id":1,"name":"Alice"}""")
    assert(!result.valid)
    assert(result.hasRawJsonEcho)
  }

}
