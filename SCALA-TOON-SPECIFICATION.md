# toon4s - Scala Implementation Specification

**Token-Oriented Object Notation (TOON)** is a compact, LLM-friendly serialization format that trades JSON's repeated keys and punctuation for indentation, column headers, and explicit row counts. This document describes the Scala implementation (`toon4s`), its guarantees, and how the format behaves on the JVM.

> If you want the canonical language-agnostic definition, read [toon-format/spec](https://github.com/toon-format/spec). This spec focuses on the Scala API, data model, and rules enforced by `toon4s`.

## Why TOON for Scala teams?

- **Token savings** mean cheaper LLM calls. TOON payloads routinely cut 30–60% of tokens versus pretty JSON.
- **Scala-first APIs** let you pass case classes, immutable maps, Options, and `java.time` values directly—no JSON intermediary required.
- **Deterministic behavior** (sealed ADTs, pure functions, explicit errors) makes TOON fit functional stacks (Cats, ZIO) and strict JVM services.
- **Guardrails** (length markers, tabular headers, strict indentation) improve LLM reliability when you force models to echo structured responses.

## Contents

1. [Format comparison](#format-comparison)
2. [Token benchmarks](#token-benchmarks)
3. [Encoding model](#encoding-model)
4. [Representation (`JsonValue` ADT)](#representation-jsonvalue-adt)
5. [Encoding rules](#encoding-rules)
6. [Decoding rules](#decoding-rules)
7. [Strict mode semantics](#strict-mode-semantics)
8. [Delimiters & length markers](#delimiters--length-markers)
9. [CLI behavior](#cli-behavior)
10. [LLM prompt usage](#llm-prompt-usage)
11. [Limitations & edge cases](#limitations--edge-cases)
12. [Conformance & testing](#conformance--testing)

---

## Format comparison

| Format | Strengths | Weaknesses vs TOON |
| ------ | --------- | ------------------ |
| JSON | ubiquitous, tooling everywhere | verbose (repeated keys, braces), harder to inspect in prompts |
| YAML | concise structure, comments | indentation-sensitive but still repeats keys for arrays |
| CSV/TSV | minimal tokens for flat tables | no nested structure, cannot represent objects |
| **TOON** | indentation for nesting, tabular headers for uniform arrays, length markers for guardrails | best when arrays share schema; falls back to list syntax for irregular rows |

When an array is uniform (same keys per row) TOON uses CSV-style rows and avoids repeating keys. For ragged arrays it falls back to YAML-like list entries, so JSON can be cheaper; use `Toon.encode` and inspect the output when in doubt.

---

## Token benchmarks

Measured with the GPT `o200k_base` tokenizer using `jtokkit`:

```
GitHub Repositories (uniform objects)     ██████████████░░░░░░░░░░░    8,745 (TOON)
                                          vs JSON: 15,145  (-42%)
                                          vs XML : 17,095  (-49%)

Daily Analytics Feed (rows with numbers)  ██████████░░░░░░░░░░░░░░░    4,507 (TOON)
                                          vs JSON:  9,234  (-51%)
```

Savings vary with schema complexity, but any payload where you repeat columns across hundreds of entries benefits immediately.

---

## Encoding model

`Toon.encode(value, EncodeOptions)` performs two steps:

1. **Normalization** – Convert arbitrary Scala values to the `JsonValue` ADT (`Normalize.toJson`).
2. **Rendering** – Convert the `JsonValue` tree to TOON text using `Encoders.encode`.

```mermaid
flowchart LR
    scala["Scala input\n(Map / Product / Iterable)"]
    normalize["Normalize.toJson\n(JsonValue)"]
    encode["Encoders.encode\n(pure renderer)"]
    toon["TOON text\n(length markers, headers)"]
    llm["LLM prompt\n(cheaper tokens)"]

    scala --> normalize --> encode --> toon --> llm

    style scala fill:#e1f5ff,stroke:#0066cc,color:#000
    style normalize fill:#fff4e1,stroke:#cc8800,color:#000
    style encode fill:#f0e1ff,stroke:#8800cc,color:#000
    style toon fill:#e1ffe1,stroke:#2d7a2d,color:#000
    style llm fill:#ffe1e1,stroke:#cc0000,color:#000
```

Normalization rules (selected):

| Scala input | JsonValue output |
| ----------- | ---------------- |
| `String` | `JString` |
| `Boolean` | `JBool` |
| Numeric primitives, `BigDecimal`, `BigInt` | `JNumber` (exact precision) |
| `Option[A]` | `JNull` for `None`, otherwise convert the value |
| `Map[String, _]`, `VectorMap` | `JObj` preserving iteration order |
| `Iterable[_]`, `Array[_]` | `JArray` |
| `Product` (case classes, tuples) | `JObj` using `productElementNames` |
| `java.time.*` (`Instant`, `LocalDate`, …) | ISO-8601 strings |
| `TemporalAccessor` without instant info | `JString(DateTimeFormatter.ISO_DATE_TIME.format)` fallback |

> Non-finite doubles/floats (`NaN`, `±Inf`) normalize to `JNull` to keep deterministic output.

---

## Representation (`JsonValue` ADT)

```scala
sealed trait JsonValue
object JsonValue {
  final case class JString(value: String)                    extends JsonValue
  final case class JNumber(value: BigDecimal)                extends JsonValue
  final case class JBool(value: Boolean)                     extends JsonValue
  case object JNull                                          extends JsonValue
  final case class JArray(value: Vector[JsonValue])          extends JsonValue
  final case class JObj(value: VectorMap[String, JsonValue]) extends JsonValue
}
```

This ADT underpins both encoding and decoding. Pattern match on it to extract fields, or call `SimpleJson.stringify` to hop back to JSON.

```mermaid
graph TD
    root["JsonValue"]
    jstr["JString\nString"]
    jnum["JNumber\nBigDecimal"]
    jbool["JBool\nBoolean"]
    jnull["JNull"]
    jarr["JArray\nVector[JsonValue]"]
    jobj["JObj\nVectorMap[String, JsonValue]"]

    root --> jstr
    root --> jnum
    root --> jbool
    root --> jnull
    root --> jarr
    root --> jobj

    style root fill:#e1f5ff,stroke:#0066cc,color:#000
    style jstr fill:#fff4e1,stroke:#cc8800,color:#000
    style jnum fill:#fff4e1,stroke:#cc8800,color:#000
    style jbool fill:#fff4e1,stroke:#cc8800,color:#000
    style jnull fill:#fff4e1,stroke:#cc8800,color:#000
    style jarr fill:#f0e1ff,stroke:#8800cc,color:#000
    style jobj fill:#f0e1ff,stroke:#8800cc,color:#000
```

---

## Encoding rules

1. **Objects** become indentation-based blocks:
   ```
   user:
     id: 123
     name: Ada
   ```
2. **Primitive arrays** default to inline syntax:
   `tags[3]: reading,gaming,coding`
3. **Tabular arrays** surface when an array of objects shares identical keys. A header and row count precede the rows:
   ```
   users[2]{id,name,role}:
     1,Ada,admin
     2,Bob,user
   ```
4. **Nested tabular arrays** declare their own headers inside the parent row:
   ```
   orders[1]{id,items}:
     1001,[2]{sku,qty}:
       A1,2
       B2,1
   ```
5. **Length markers (`#`)** are optional but recommended for LLM prompts: `items[#2|]{sku|qty}`.
6. **Delimiters** default to comma. Tab (`\t`) or pipe (`|`) can be selected via `EncodeOptions`. The delimiter character appears after the length / inside the braces to keep readers honest (`[2\t]{a\tb}` or `[2|]{a|b}`).
7. **Quoting rules** mirror JSON: quote strings when they contain whitespace, delimiters, `:`, `"`, `,`, `[`, `]`, `{`, `}`, or start with `- `.
8. **Empty structures**: `prefs[0]:` (empty array) or `prefs: {}` (empty object).

Inline vs block selection:

- Primitive arrays -> inline unless huge (controlled upstream).
- Object arrays -> tabular when keys align, else fallback list syntax:
  ```
  tasks:
    - id: 1
      name: sync
    - id: 2
      tags[1]: urgent
  ```

---

## Decoding rules

`Toon.decode(string, DecodeOptions)` parses the input to `JsonValue` or returns `DecodeError`:

- `Syntax` – malformed indentation, unterminated strings, unknown escape sequences, missing colon, etc.
- `Range` – row count mismatch, extra tabular rows, list item overflows.
- `InvalidHeader` – bracket/brace segments misdeclared.
- `Unexpected` – catch-all for parser bugs.

Decoder algorithm highlights:

1. **Indentation scanner** tokenizes lines into `ParsedLine` (depth implied by spaces). Tabs are forbidden in strict mode.
2. **Array header detection** checks for `[length]{fields}` patterns, capturing delimiters and markers.
3. **Delimited rows** split on the negotiated delimiter, respecting quotes and escapes.
4. **Primitive detection** chooses between strings, booleans, null, or numbers based on literal regexes.
5. **Validation hooks** ensure row/field counts match the header when `strict = true`.

```mermaid
flowchart TD
    raw["TOON text"]
    scan["Scanner\n(indent -> depth)"]
    parse["Parser\n(headers, rows)"]
    validate["Validation hooks\n(strict mode)"]
    json["JsonValue tree"]

    raw --> scan --> parse --> validate --> json

    style raw fill:#e1f5ff,stroke:#0066cc,color:#000
    style scan fill:#fff4e1,stroke:#cc8800,color:#000
    style parse fill:#f0e1ff,stroke:#8800cc,color:#000
    style validate fill:#ffe1e1,stroke:#cc0000,color:#000
    style json fill:#e1ffe1,stroke:#2d7a2d,color:#000
```

---

## Strict mode semantics

`DecodeOptions(strict = true)` (default) enforces:

- Indentation must be multiples of `indent` (default `2` spaces).
- No tab characters before content.
- Tabular rows must match header field count.
- Tabular arrays cannot contain blank lines between rows.
- List arrays cannot have extra `-` entries beyond declared length.
- Inline array lengths must match actual element count.

Relaxing `strict` tolerates indentation quirks and length mismatches but still rejects syntax errors (unterminated strings, invalid escapes, etc.).

---

## Delimiters & length markers

- `EncodeOptions.delimiter`: `Delimiter.Comma` | `Delimiter.Tab` | `Delimiter.Pipe`.
- `EncodeOptions.lengthMarker`: when `true`, headers use `[#N]` instead of `[N]`. Combined with alternate delimiters: `items[#2|]{sku|qty}` or `tags[#3\t]: a\tb\tc`.

Choosing delimiters:

| Delimiter | When to use |
| --------- | ----------- |
| Comma | Default, human-friendly, works for most data. |
| Tab | Token-efficient for numeric-heavy tables, rarely needs quoting. |
| Pipe | Easier to eyeball when values already contain commas or tabs. |

Length markers should be enabled for LLM prompts or whenever you want the decoder/model to prove row counts.

```mermaid
flowchart LR
    opts["EncodeOptions\n(indent, delimiter, lengthMarker)"]
    header["Header builder\n[ len ] { fields }"]
    rows["Row renderer\n(split w/ delimiter)"]
    output["TOON stream"]

    opts --> header --> rows --> output

    style opts fill:#e1f5ff,stroke:#0066cc,color:#000
    style header fill:#fff4e1,stroke:#cc8800,color:#000
    style rows fill:#f0e1ff,stroke:#8800cc,color:#000
    style output fill:#e1ffe1,stroke:#2d7a2d,color:#000
```

---

## CLI behavior

`toon4s-cli` provides:

- `--encode <file>` / `--decode <file>` (direction)
- `--indent <n>`
- `--delimiter comma|tab|pipe`
- `--length-marker`
- `--strict true|false`
- `--output <file>` (stdout fallback)

It uses the same `SimpleJson` helper for JSON round-trips, ensuring CLI ↔ library parity. `TokenEstimator` (jtokkit, CL100K base) will surface in future releases to log token counts after each conversion.

```mermaid
flowchart LR
    user["Command line\n(toon4s-cli)"]
    scopt["scopt parser\nflags + config"]
    branch{"Mode?"}
    encode["Encode path\nJSON -> TOON"]
    decode["Decode path\nTOON -> JSON"]
    filesystem["Files/stdout"]

    user --> scopt --> branch
    branch -->|--encode| encode --> filesystem
    branch -->|--decode| decode --> filesystem

    style user fill:#e1f5ff,stroke:#0066cc,color:#000
    style scopt fill:#fff4e1,stroke:#cc8800,color:#000
    style branch fill:#f0e1ff,stroke:#8800cc,color:#000
    style encode fill:#e1ffe1,stroke:#2d7a2d,color:#000
    style decode fill:#ffe1e1,stroke:#cc0000,color:#000
    style filesystem fill:#d1c4e9,stroke:#5e35b1,color:#000
```

---

## LLM prompt usage

Basic prompt template:

```
System: You are a strict validator. Respect row counts.
User:
orders[#3]{id,total,status}:
  101,250.10,pending
  102,89.00,fulfilled
  103,140.00,review
```

Guidelines:

1. Always send row counts/headers so the model knows expected output cardinality.
2. Ask the model to respond in TOON or JSON, then parse with `Toon.decode` or `SimpleJson.parse`.
3. Validate model output by comparing array lengths and field names.
4. Prefer tab delimiters for numeric-heavy tables; they tokenize cheaper.

```mermaid
sequenceDiagram
    participant S as Scala app
    participant L as LLM
    S->>S: Toon.encode(payload)
    S->>L: Send TOON prompt\n(with length markers)
    L-->>S: Structured reply
    S->>S: Toon.decode / SimpleJson.parse
    S->>S: Validate lengths & fields
```

---

## Limitations & edge cases

- **Irregular arrays** degrade to list syntax (less compact).
- **Binary / large blobs** should be base64 before encoding.
- **Streaming** not implemented; `Toon.decode` expects whole strings in memory.
- **Java interop**: `toon4s` exposes only Scala collections. For Java call sites, wrap `Toon.encode`/`decode` with conversion helpers.
- **Non-scalar map keys** get `toString`'d; prefer `Map[String, _]` or `VectorMap`. 

---

## Conformance & testing

- `core/src/test/.../ConformanceSuite.scala` loads fixtures from `spec/tests/fixtures` and asserts parity with the reference TS implementation.
- `DecodeSpec` covers parser edge cases (blank lines, strict mode errors).
- Smoke tests stage the CLI, run JSON→TOON→JSON round trips, and compare payloads.
- CI matrix executes `sbt -Dsbt.log.noformat=true ++<scalaVersion> test` on Linux, macOS, Windows.

You can run the full suite locally:

```bash
sbt scalafmtCheckAll
sbt +test
./smoke-tests/run-smoke.sh
```

---

## See also

- [Main README](./README.md) – installation, usage, CLI examples.
- [toon-format/spec](https://github.com/toon-format/spec) – canonical format specification & fixtures.
- [JToon](https://github.com/felipestanzani/jtoon) – JVM reference in Kotlin/Java.
- [Original TypeScript implementation](https://github.com/toon-format/toon).
