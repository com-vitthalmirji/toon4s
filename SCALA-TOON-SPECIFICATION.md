# TOON specification (Scala alignment)

The canonical, language-agnostic specification now lives at
[toon-format/spec](https://github.com/toon-format/spec). `toon4s` tracks that
source of truth and targets **TOON v3.3** (2026-05-21), while keeping
the decoder lenient for legacy v2.x layouts where safe.

## Spec version history

| Version | Key changes in toon4s |
|---|---|
| v3.3 (current) | Lowercase bool/null MUST; canonical-decimal range scoped; `indentSize` option name |
| v3.2 | Duplicate-key strict error; header delimiter mismatch error; malformed bracket rejection; `{}` arrays use expanded list; nested array-of-objects list form |
| v3.1 | `\uXXXX` escape (encoder emits, decoder accepts); empty array canonical form `key: []` |
| v3.0 | Tabular-first-field list-item depth rules; `[#N]` removed |
| v2.x | Key folding, path expansion, strict mode |

## Option name mapping (spec §13)

The spec uses concept-handle names; toon4s uses Scala-idiomatic field names (allowed by §13):

| Spec name | toon4s field | Notes |
|---|---|---|
| `indentSize` | `indent` | `EncodeOptions.indent`, `DecodeOptions.indent` |
| `strict` | `strictness` | `DecodeOptions.strictness = Strictness.Strict` (default) |
| `delimiter` | `delimiter` | `EncodeOptions.delimiter`, `Delimiter.Comma/Tab/Pipe` |
| `keyFolding` | `keyFolding` | `EncodeOptions.keyFolding = KeyFolding.Safe` |
| `flattenDepth` | `flattenDepth` | `EncodeOptions.flattenDepth` |
| `expandPaths` | `expandPaths` | `DecodeOptions.expandPaths = PathExpansion.Safe` |

## Media type

The provisional IANA media type for TOON is `text/toon` (spec §17). File extension: `.toon`.
Charset is always UTF-8; `charset=utf-8` may be specified and is assumed if absent.

## Compatibility note

toon4s emits the v3.0-required row depth (`+2`) for tabular arrays placed as
the first field of list-item objects, and uses `key: []` for empty arrays per v3.1.

## Where to look

- Full spec: https://github.com/toon-format/spec/blob/main/SPEC.md
- Changelog: https://github.com/toon-format/spec/blob/main/CHANGELOG.md
- Conformance fixtures: synced from `tests/fixtures` at commit `07161ccc` (v3.3 release).

`toon4s` implements the Scala/JVM interpretation of that spec (encoding, decoding, CLI) while
maintaining deterministic behavior, strict mode validation, and zero-dependency core.

## JVM host-type normalization policies (spec §3, Appendix F.5)

The spec requires implementations to document how host-specific types map to the JSON data model
before encoding. toon4s policies are listed below.

### GAP-S01: `BigDecimal` numeric precision

`JNumber` wraps a `scala.math.BigDecimal`. Construct it from a **string** literal to preserve
full precision:

```scala
JNumber(BigDecimal("0.1"))   // lossless: exact decimal 0.1
JNumber(BigDecimal(0.1))     // lossy: IEEE 754 double 0.1000000000000000055...
```

For out-of-range or arbitrary-precision values the encoder emits a quoted decimal string
(plain decimal form, per spec §2). toon4s does not expose a `stringifyOutOfRange` option;
callers must wrap the value in `JString` manually when lossless string representation is needed.

### GAP-S02: `java.time.ZonedDateTime`

`ZonedDateTime.toString()` appends a zone-id suffix (e.g. `[Europe/Berlin]`) that is not valid
ISO 8601 for interchange. Always convert before encoding:

```scala
zdt.toOffsetDateTime().toString   // correct: "2026-06-02T15:00:00+02:00"
zdt.toString                      // wrong:   "2026-06-02T15:00:00+02:00[Europe/Berlin]"
```

### GAP-S03: `Option` / `java.util.Optional`

- `None` / `Optional.empty()` → **omit the key entirely** (do not emit `null`).
- `Some(x)` / `Optional.of(x)` → encode `x` directly.

There is no built-in typeclass for `Option`; apply the convention when constructing `JObj`.

### GAP-S04: `Map[K, V]` with non-String keys

Keys are coerced to `String` via `.toString`. For numeric or structured key types, define a
custom `KeyEncoder` typeclass in your application layer to control the coercion. Encoded key
order follows `VectorMap` insertion order (encounter order is preserved).

### GAP-S05: `Set[T]` iteration order

`scala.collection.Set` has **undefined** iteration order on the JVM. Encoding a `Set` produces
non-deterministic field/element order across JVM runs. If deterministic output is required,
sort the set before encoding or use a sorted collection type.

## Upgrading from earlier versions (1.4 / 2.x)

- `[#N]` length markers: removed in spec v2.0; decoder now rejects them, encoder does not emit them.
- Row depth: list-item tabular arrays emit rows at depth `+2` (v3 layout).
- New optional features: key folding (`keyFolding="safe"`, `flattenDepth`) and path expansion (`expandPaths="safe"`) are off by default for backward compatibility.
- Empty arrays: canonical form is now `key: []` (encoder) and `key: []` is accepted on decode (v3.1).

## Scala implementation architecture

### Pure functional design

toon4s implements the TOON spec with pure functional programming principles:

**Pure functions**: All encode/decode operations are referentially transparent with no side effects. The API returns `Either[DecodeError, JsonValue]` instead of throwing exceptions, enabling composability with Cats, ZIO, and other FP libraries.

**Immutable ADTs**: The `JsonValue` sealed trait provides exhaustive pattern matching over `JNull`, `JBool`, `JNumber`, `JString`, `JArray`, and `JObj`. Objects use `VectorMap` for deterministic field ordering.

**Type safety**: Scala 3 derivation via `Encoder.derived` and `Decoder.derived` provides compile-time guarantees. Scala 2.13 users get equivalent safety through `ToonTyped` typeclasses.

**Stack safety**: All recursive operations use tail recursion or trampolining. The visitor pattern and cursor navigation are stack-safe, handling arbitrarily deep structures within configured limits.

### Performance with purity

toon4s achieves **2x performance improvement** while maintaining functional purity:

**Zero-allocation patterns**:
- Pre-allocated `StringBuilder` capacity based on estimated output size
- Single-pass string processing (combined quote-finding + unescaping)
- Cached common patterns (array headers for lengths 0-10)
- `VectorBuilder` with while loops instead of functional chains

**Hot-path optimization**:
- Direct character operations instead of string allocations
- Pattern matching for delimiter dispatch
- Early-exit evaluation with `iterator.forall`
- Hoisted constants outside loops

**Memory efficiency**:
- Streaming visitors with O(depth) memory usage
- No intermediate allocations in visitor chains
- Tail-recursive iteration for large arrays
- Stack-safe cursor navigation

### Visitor pattern architecture

The visitor pattern enables zero-overhead transformations:

**Universal TreeWalker**: Adapts external JSON libraries (Jackson, Circe, Play JSON) without converting to intermediate `JsonValue` representation.

**Composable visitors**: Chain multiple visitors (`FilterKeysVisitor`, `JsonRepairVisitor`, `StringifyVisitor`) in a single pass with O(1) memory overhead.

**Streaming guarantees**: Process millions of rows with constant memory using `foreachTabular` and `foreachArrays`, which iterate without building full ASTs.

### Type-driven development

toon4s leverages Scala's type system for correctness:

**Compile-time validation**: Encoder/Decoder derivation catches schema mismatches at compile time, not runtime.

**Sealed ADTs**: Exhaustive pattern matching ensures all `JsonValue` cases are handled, preventing runtime errors.

**Phantom types**: Configuration types like `Strictness` and `KeyFolding` use sealed traits to restrict valid values at compile time.

**Zero-cost abstractions**: Type-level programming and inline optimizations ensure abstraction overhead is eliminated by the compiler.
