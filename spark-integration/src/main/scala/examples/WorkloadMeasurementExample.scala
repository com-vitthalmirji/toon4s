package examples

/** Backward-compatible entry alias. Prefer WorkloadMeasurementRunner for new runs. */
object WorkloadMeasurementExample {

  def main(args: Array[String]): Unit =
    WorkloadMeasurementRunner.main(args)

}
