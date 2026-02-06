package io.toonformat.toon4s.playground

import scala.util.Try

import io.toonformat.toon4s._
import io.toonformat.toon4s.json.SimpleJson
import org.scalajs.dom
import org.scalajs.dom.{document, window}

/**
 * Web playground application for toon4s.
 *
 * Provides an interactive interface for converting between JSON and TOON formats, demonstrating
 * token savings and format features.
 */
object PlaygroundApp {

  /** Main entry point for the Scala.js application. */
  def main(args: Array[String]): Unit = {
    dom.window.addEventListener(
      "load",
      { (_: dom.Event) =>
        setupEventListeners()
        loadExampleData()
        ()
      },
    )
  }

  /** Setup event listeners for UI elements. */
  private def setupEventListeners(): Unit = {
    // Convert JSON to TOON button
    getElement("convertToToon").foreach { btn =>
      btn.addEventListener(
        "click",
        { (_: dom.Event) =>
          convertJsonToToon()
          ()
        },
      )
    }

    // Convert TOON to JSON button
    getElement("convertToJson").foreach { btn =>
      btn.addEventListener(
        "click",
        { (_: dom.Event) =>
          convertToonToJson()
          ()
        },
      )
    }

    // Load example buttons
    getElement("loadExample1").foreach { btn =>
      btn.addEventListener(
        "click",
        { (_: dom.Event) =>
          loadExample(example1)
          ()
        },
      )
    }

    getElement("loadExample2").foreach { btn =>
      btn.addEventListener(
        "click",
        { (_: dom.Event) =>
          loadExample(example2)
          ()
        },
      )
    }

    getElement("loadExample3").foreach { btn =>
      btn.addEventListener(
        "click",
        { (_: dom.Event) =>
          loadExample(example3)
          ()
        },
      )
    }

    // Format selector
    getElement("delimiter").foreach { select =>
      select.addEventListener(
        "change",
        { (_: dom.Event) =>
          convertJsonToToon()
          ()
        },
      )
    }

    getElement("indent").foreach { select =>
      select.addEventListener(
        "change",
        { (_: dom.Event) =>
          convertJsonToToon()
          ()
        },
      )
    }

    // Clear buttons
    getElement("clearJson").foreach { btn =>
      btn.addEventListener(
        "click",
        { (_: dom.Event) =>
          getTextArea("jsonInput").foreach(_.value = "")
          clearOutput()
          ()
        },
      )
    }

    getElement("clearToon").foreach { btn =>
      btn.addEventListener(
        "click",
        { (_: dom.Event) =>
          getTextArea("toonInput").foreach(_.value = "")
          clearOutput()
          ()
        },
      )
    }
  }

  /** Convert JSON input to TOON format. */
  private def convertJsonToToon(): Unit = {
    getTextArea("jsonInput").foreach { textarea =>
      val jsonInput = textarea.value

      if (jsonInput.trim.isEmpty) {
        showError("Please enter some JSON data")
      } else {
        val result = for {
          parsed <- Try(SimpleJson.parse(jsonInput)).toEither.left.map(t =>
            s"Invalid JSON: ${t.getMessage}"
          )
          scalaValue <- Try(SimpleJson.toScala(parsed)).toEither.left.map(t =>
            s"Conversion error: ${t.getMessage}"
          )
          delimiter <- getSelectedDelimiter()
          indent <- getSelectedIndent()
          options = EncodeOptions(delimiter = delimiter, indent = indent)
          toonOutput <- Toon.encode(scalaValue, options).left.map(_.message)
        } yield (jsonInput, toonOutput)

        result match {
        case Right((json, toon)) =>
          getTextArea("toonOutput").foreach(_.value = toon)
          clearError()
          updateStats(json, toon)
        case Left(error) =>
          showError(error)
          clearOutput()
        }
      }
    }
  }

  /** Convert TOON input to JSON format. */
  private def convertToonToJson(): Unit = {
    getTextArea("toonInput").foreach { textarea =>
      val toonInput = textarea.value

      if (toonInput.trim.isEmpty) {
        showError("Please enter some TOON data")
      } else {
        val result = for {
          jsonValue <- Toon.decode(toonInput).left.map(_.message)
          jsonOutput = SimpleJson.stringify(jsonValue)
        } yield (toonInput, jsonOutput)

        result match {
        case Right((toon, json)) =>
          getTextArea("jsonOutput").foreach(_.value = json)
          clearError()
          updateStats(json, toon)
        case Left(error) =>
          showError(error)
          clearOutput()
        }
      }
    }
  }

  /** Update statistics display showing token counts and savings. */
  private def updateStats(jsonText: String, toonText: String): Unit = {
    // Simple character-based estimation (approximates token count)
    val jsonChars = jsonText.length
    val toonChars = toonText.length
    val savings = if (jsonChars > 0) {
      ((jsonChars - toonChars).toDouble / jsonChars * 100).toInt
    } else 0

    getElement("statsJsonSize").foreach { elem => elem.textContent = s"$jsonChars chars" }
    getElement("statsToonSize").foreach { elem => elem.textContent = s"$toonChars chars" }
    getElement("statsSavings").foreach { elem =>
      elem.textContent = if (savings > 0) s"$savings%" else "0%"
      elem.className = if (savings > 0) "savings-positive" else "savings-neutral"
    }

    getElement("statsSection").foreach { section => section.style.display = "block" }
  }

  /** Get selected delimiter from UI. */
  private def getSelectedDelimiter(): Either[String, Delimiter] = {
    getSelect("delimiter") match {
    case Some(select) =>
      select.value match {
      case "comma" => Right(Delimiter.Comma)
      case "tab"   => Right(Delimiter.Tab)
      case "pipe"  => Right(Delimiter.Pipe)
      case other   => Left(s"Unknown delimiter: $other")
      }
    case None => Left("Delimiter selector not found")
    }
  }

  /** Get selected indent from UI. */
  private def getSelectedIndent(): Either[String, Int] = {
    getSelect("indent") match {
    case Some(select) =>
      Try(select.value.toInt).toEither.left.map(t => s"Invalid indent: ${t.getMessage}")
    case None => Left("Indent selector not found")
    }
  }

  /** Load an example into the JSON input. */
  private def loadExample(exampleJson: String): Unit = {
    getTextArea("jsonInput").foreach { textarea =>
      textarea.value = exampleJson
      convertJsonToToon()
    }
  }

  /** Load initial example data. */
  private def loadExampleData(): Unit = {
    loadExample(example1)
  }

  /** Show error message in the UI. */
  private def showError(message: String): Unit = {
    getElement("errorMessage").foreach { elem =>
      elem.textContent = message
      elem.style.display = "block"
    }
  }

  /** Clear error message. */
  private def clearError(): Unit = {
    getElement("errorMessage").foreach { elem =>
      elem.textContent = ""
      elem.style.display = "none"
    }
  }

  /** Clear all output areas. */
  private def clearOutput(): Unit = {
    getTextArea("toonOutput").foreach(_.value = "")
    getTextArea("jsonOutput").foreach(_.value = "")
    getElement("statsSection").foreach(_.style.display = "none")
  }

  /** Helper to get an element by ID. */
  private def getElement(id: String): Option[dom.html.Element] = {
    Option(document.getElementById(id).asInstanceOf[dom.html.Element])
  }

  /** Helper to get a textarea element by ID. */
  private def getTextArea(id: String): Option[dom.html.TextArea] = {
    Option(document.getElementById(id).asInstanceOf[dom.html.TextArea])
  }

  /** Helper to get a select element by ID. */
  private def getSelect(id: String): Option[dom.html.Select] = {
    Option(document.getElementById(id).asInstanceOf[dom.html.Select])
  }

  // Example data
  private val example1 = """{
  "users": [
    {
      "id": 1,
      "name": "Alice",
      "email": "alice@example.com",
      "tags": ["admin", "developer"]
    },
    {
      "id": 2,
      "name": "Bob",
      "email": "bob@example.com",
      "tags": ["user"]
    }
  ]
}"""

  private val example2 = """{
  "orders": [
    {
      "id": 1001,
      "user": "alice",
      "total": 29.70,
      "items": [
        {"sku": "A1", "qty": 2, "price": 9.99},
        {"sku": "B2", "qty": 1, "price": 5.50},
        {"sku": "C3", "qty": 1, "price": 4.22}
      ]
    },
    {
      "id": 1002,
      "user": "bob",
      "total": 15.00,
      "items": [
        {"sku": "D4", "qty": 1, "price": 15.00}
      ]
    }
  ]
}"""

  private val example3 = """{
  "product": {
    "id": "P-12345",
    "name": "Wireless Headphones",
    "price": 79.99,
    "inStock": true,
    "categories": ["electronics", "audio", "accessories"],
    "specs": {
      "battery": "20 hours",
      "bluetooth": "5.0",
      "weight": "250g"
    }
  }
}"""

}
