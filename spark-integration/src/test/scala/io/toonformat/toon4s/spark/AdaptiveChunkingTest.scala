package io.toonformat.toon4s.spark

import munit.FunSuite

class AdaptiveChunkingTest extends FunSuite {

  test("calculateOptimalChunkSize: handles huge row count safely") {
    val strategy = AdaptiveChunking.calculateOptimalChunkSize(
      totalRows = Int.MaxValue.toLong + 1000L,
      avgRowSize = 0,
    )

    assert(strategy.useToon)
    assert(strategy.chunkSize >= 100)
    assert(strategy.chunkSize <= 1000)
  }

  test("calculateOptimalChunkSize: keeps large dataset chunk under cap") {
    val strategy = AdaptiveChunking.calculateOptimalChunkSize(
      totalRows = Int.MaxValue.toLong + 1000L,
      avgRowSize = 1,
    )

    assert(strategy.useToon)
    assert(strategy.chunkSize >= 100)
    assert(strategy.chunkSize <= 1000)
  }

  test("calculateOptimalChunkSize: saturates estimated size on overflow") {
    val strategy = AdaptiveChunking.calculateOptimalChunkSize(
      totalRows = Long.MaxValue,
      avgRowSize = Int.MaxValue,
    )

    assert(strategy.useToon)
    assertEquals(strategy.estimatedDataSize, Long.MaxValue)
    assert(strategy.chunkSize >= 100)
    assert(strategy.chunkSize <= 1000)
  }

}
