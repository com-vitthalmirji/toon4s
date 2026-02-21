package io.toonformat.toon4s.spark

import munit.FunSuite

class ExampleEntryPointsTest extends FunSuite {

  private val expectedEntryPoints = List(
    "examples.WorkloadMeasurementRunner$",
    "examples.WorkloadMeasurementExample$",
  )

  test("documented example entry points exist on classpath") {
    expectedEntryPoints.foreach { entryPoint =>
      assert(
        scala.util.Try(Class.forName(entryPoint)).isSuccess,
        s"Missing entry point: $entryPoint",
      )
    }
  }

}
