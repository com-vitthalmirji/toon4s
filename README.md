# toon4s - Token-Oriented Object Notation for Scala

`toon4s` is the Scala 3 implementation of [Token-Oriented Object Notation (TOON)](https://github.com/toon-format/spec),
a compact, LLM-friendly data format that blends YAML-style structure with CSV-like tabular efficiency.
It mirrors the behaviour and tests of the reference TypeScript implementation (`toon`) and the JVM
reference port (`JToon`), while offering idiomatic Scala APIs and a zero-dependency core.

## Highlights

- **Spec-complete** - passes the full language-agnostic conformance suite from `toon-format/spec`.
- **Type-safe core** - exposes a sealed `JsonValue` ADT (`JString`, `JNumber`, `JBool`, `JNull`,
  `JArray`, `JObj`) and pure encoder/decoder functions returning `Either[ToonError, *]`.
- **Zero third-party dependencies** - core functionality relies only on the Scala standard library.
- **Deterministic ordering** - object fields preserve insertion order for predictable output.
- **CLI included** - provides the same ergonomics as the TypeScript CLI for converting JSON ⇄ TOON.
- **Smoke-tested** - real-world round trip script included (`./smoke-tests/run-smoke.sh`).

## Modules

| Module         | Description                                             |
| -------------- | ------------------------------------------------------- |
| `toon4s-core`  | Library: encode/decode, JSON normalisation, conformance |
| `toon4s-cli`   | CLI wrapper around the core encoder/decoder             |

## Installation (library)

Add the dependency to your `build.sbt`:

```scala
libraryDependencies += "io.toonformat" %% "toon4s-core" % "0.1.0"
```

### Quick start

```scala
import io.toonformat.toon4s.{Toon, EncodeOptions, DecodeOptions}

val data = Map(
  "users" -> Vector(
    Map("id" -> 1, "name" -> "Ada", "tags" -> Vector("reading", "gaming")),
    Map("id" -> 2, "name" -> "Bob", "tags" -> Vector("writing"))
  )
)

val encoded = Toon.encode(data, EncodeOptions(indent = 2)).fold(throw _, identity)
println(encoded)
// users[2]{id,name,tags}:
//   1,Ada,[2]: reading,gaming
//   2,Bob,[1]: writing

val decoded = Toon.decode(encoded, DecodeOptions(strict = true)).fold(throw _, identity)
println(decoded)
```

The encoder normalises arbitrary Scala structures (maps, case classes, iterables, options) into
`JsonValue` before producing TOON output. The decoder returns a `JsonValue` tree that you can walk or
convert back to Scala structures using helpers in `io.toonformat.toon4s.json.SimpleJson`.

## CLI usage

The CLI mirrors the behaviour of the TypeScript tool while remaining JVM-native.

```bash
# Stage an executable (cached by sbt)
sbt cli/stage

# Encode JSON -> TOON
./cli/target/universal/stage/bin/toon4s-cli --encode data.json -o data.toon

# Decode TOON -> JSON
./cli/target/universal/stage/bin/toon4s-cli --decode data.toon -o roundtrip.json
```

Key flags:

| Flag             | Description                                           |
| ---------------- | ----------------------------------------------------- |
| `--encode`       | Force JSON → TOON conversion (auto-detected otherwise)|
| `--decode`       | Force TOON → JSON conversion                          |
| `--indent N`     | Indentation width (default `2`)                       |
| `--delimiter`    | Comma `,`, tab `\t`, or pipe `|` delimiters           |
| `--lengthMarker` | Emit length markers (`[#N]`)                          |
| `--no-strict`    | Relax strict decoder validation                       |
| `--stats`        | Estimate GPT token counts before/after conversion     |

The CLI uses the same pure Scala JSON codec (`SimpleJson`) as the conformance suite – no external `jq`
or Jackson dependency is required.

## Testing & quality gates

```bash
# Run unit + conformance tests
sbt test

# Execute the CLI smoke test round trip
./smoke-tests/run-smoke.sh
```

The test suite executes all fixtures copied from `toon-format/spec/tests/fixtures`, ensuring parity
with the TypeScript and Java implementations. The smoke script runs a staged CLI binary against a
structured JSON payload and verifies round-trip fidelity.

## Continuous integration

A GitHub Actions workflow (`.github/workflows/ci.yml`) runs `sbt test` on every push and pull request.
The workflow caches the sbt/ivy directories for incremental builds.

## Project structure

```
.
├── core/               # Library sources, conformance suite, JSON codec
├── cli/                # CLI entry point and packaging
├── smoke-tests/        # Manual smoke-test script and docs
├── docs/               # Roadmap and design documentation
├── project/            # sbt plugins
├── README.md           # This file (Scala-focused docs)
├── CHANGELOG.md        # Version history
└── .github/workflows/  # CI pipeline
```

## Releasing (maintainers)

Tagged releases (`v*`) trigger `.github/workflows/release.yml`, which:

1. Runs the full test suite and CLI smoke tests.
2. Publishes artifacts to the configured repository (`sonatype` by default, `jfrog` when `PUBLISH_TARGET=jfrog`).
3. Packages the CLI (ZIP) and creates a GitHub Release with git-cliff release notes.

### Required secrets / env vars

| Secret | Description |
| ------ | ----------- |
| `SONATYPE_USERNAME` / `SONATYPE_PASSWORD` | OSSRH credentials (used when `PUBLISH_TARGET` is unset or `sonatype`). |
| `PGP_SECRET` | Base64-encoded ASCII-armored private key for signing. |
| `PGP_PASSPHRASE` | Passphrase for the signing key. |
| `PUBLISH_TARGET` | Optional. Set to `jfrog` to target an Artifactory repo. |
| `PUBLISH_USER` / `PUBLISH_PASS` | Optional override for publishing credentials (if you prefer generic names). |
| `SONATYPE_HOST` | Optional alternate Sonatype host (defaults to `s01.oss.sonatype.org`). |
| `JFROG_HOST`, `JFROG_USER`, `JFROG_PASSWORD`, `JFROG_REPO_URL` | Optional; used when `PUBLISH_TARGET=jfrog`. |

Before tagging, bump `version` in `build.sbt` and ensure `CHANGELOG.md` is up to date (the changelog workflow will auto-commit if needed).

## Changelog

See [CHANGELOG.md](./CHANGELOG.md).

## Roadmap

See [docs/ROADMAP.md](./docs/ROADMAP.md). For 1.0 planning issues, see [.github/ISSUES_V1.md](.github/ISSUES_V1.md).

## License

MIT — see [LICENSE](./LICENSE).
