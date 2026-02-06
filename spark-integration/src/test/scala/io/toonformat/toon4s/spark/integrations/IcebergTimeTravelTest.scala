package io.toonformat.toon4s.spark.integrations

import java.time.Instant

import munit.FunSuite

class IcebergTimeTravelTest extends FunSuite {

  test("asOfTimestampOptionValue uses epoch millis") {
    val instant = Instant.parse("2024-12-01T00:00:00Z")
    val optionValue = IcebergTimeTravel.asOfTimestampOptionValue(instant)

    assertEquals(optionValue, "1733011200000")
  }

}
