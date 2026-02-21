package io.toonformat.toon4s

import java.io.StringReader

import io.toonformat.toon4s.decode.Streaming
import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop._

class ParserFuzzSpec extends ScalaCheckSuite {

  private val genFuzzInput: Gen[String] =
    Gen.choose(0, 2048).flatMap { size =>
      Gen
        .listOfN(
          size,
          Gen.oneOf(
            Gen.alphaNumChar,
            Gen.oneOf('\n', '\r', '\t', ':', ',', '"', '-', '[', ']', '{', '}', ' ', '\u0000'),
          ),
        )
        .map(_.mkString)
    }

  property("decode does not throw on fuzz input") {
    forAll(genFuzzInput) { input =>
      val noThrow = scala.util.Try(Toon.decode(input)).isSuccess
      assert(noThrow, clues(input.take(120)))
    }
  }

  property("streaming tabular path does not throw on fuzz input") {
    forAll(genFuzzInput) { input =>
      val runResult = scala.util.Try {
        Streaming.foreachTabular(new StringReader(input)) { (_, _, _) =>
          ()
        }
      }
      val onlyDomainFailure =
        runResult.failed.toOption.exists(_.isInstanceOf[error.DecodeError])
      assert(runResult.isSuccess || onlyDomainFailure, clues(input.take(120), runResult.failed.toOption.map(_.toString)))
    }
  }

  property("streaming arrays path does not throw on fuzz input") {
    forAll(genFuzzInput) { input =>
      val runResult = scala.util.Try {
        Streaming.foreachArrays(new StringReader(input))(
          onHeader = (_, _) => (),
          onRow = (_, _, _) => (),
        )
      }
      val onlyDomainFailure =
        runResult.failed.toOption.exists(_.isInstanceOf[error.DecodeError])
      assert(runResult.isSuccess || onlyDomainFailure, clues(input.take(120), runResult.failed.toOption.map(_.toString)))
    }
  }

}
