# TOON specification (Scala alignment)

The canonical, language-agnostic specification now lives at
[toon-format/spec](https://github.com/toon-format/spec). `toon4s` tracks that
source of truth and targets **TOON v3.0.x** (latest tag: v3.0.1), while keeping
the decoder lenient for legacy v2.x layouts where safe.

## What changed since v1.4

- Removed legacy `[#N]` length markers; only `[N]` headers are valid.
- Added optional key folding (`keyFolding="safe"` with `flattenDepth`) for dotted paths.
- Added optional path expansion on decode (`expandPaths="safe"`) with strict/LWW conflict rules.
- Clarified canonical number formatting and delimiter scoping per spec sections 2 & 11.

## Compatibility note

toon4s emits the v3.0-required row depth (`+2`) for tabular arrays placed as
the first field of list-item objects and accepts legacy v2.x depths on decode.

## Where to look

- Full spec: https://github.com/toon-format/spec/blob/v3.0.1/SPEC.md
- Changelog: https://github.com/toon-format/spec/blob/v3.0.1/CHANGELOG.md
- Conformance fixtures: synced from `tests/fixtures` in the spec repo.

`toon4s` implements the Scala/JVM interpretation of that spec (encoding, decoding, CLI) while
maintaining deterministic behavior, strict mode validation, and zero-dependency core. Use the
Options in the README to enable folding/expansion features across v2/v3.

## Upgrading from earlier versions (1.4 / 2.x)

- CLI: `--strict` is deprecated; use `--strictness strict|lenient` (defaults to strict). The old flag remains temporarily.
- Length markers: legacy `[#N]` headers are no longer produced; decoders stay lenient to legacy files.
- Row depth: list-item tabular arrays emit rows at depth `+2` (v3 layout) while decoders accept legacy depths.
- New optional features: key folding (`keyFolding="safe"`, `flattenDepth`) and path expansion (`expandPaths="safe"`) are off by default for backward compatibility.

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
