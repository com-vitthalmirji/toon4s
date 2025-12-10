# TOON Specification (Scala alignment)

The canonical, language-agnostic specification now lives at
[toon-format/spec](https://github.com/toon-format/spec). `toon4s` tracks that
source of truth and targets **TOON v2.1.0** (2025-11-23), emitting the updated
list-item/tabular layout with **v3.0 row depth (+2) semantics** while keeping
the decoder lenient for legacy v2.0/v2.1 shapes.

## What changed since v1.4

- Removed legacy `[#N]` length markers; only `[N]` headers are valid.
- Added optional key folding (`keyFolding="safe"` with `flattenDepth`) for dotted paths.
- Added optional path expansion on decode (`expandPaths="safe"`) with strict/LWW conflict rules.
- Clarified canonical number formatting and delimiter scoping per spec sections 2 & 11.

## v3.0 gap (working draft)

We already emit the v3.0-required row depth (+2) for tabular arrays placed as
the first field of list-item objects. Other v3.0 draft changes remain under
review; decoders stay lenient toward legacy v2.0/v2.1 layouts.

## Where to look

- Full spec: https://github.com/toon-format/spec/blob/v2.1.0/SPEC.md
- Changelog: https://github.com/toon-format/spec/blob/v2.1.0/CHANGELOG.md
- Conformance fixtures: synced from `tests/fixtures` in the spec repo.

`toon4s` implements the Scala/JVM interpretation of that spec (encoding, decoding, CLI) while
maintaining deterministic behavior, strict mode validation, and zero-dependency core. Use the
Options in the README to enable folding/expansion features introduced in v2.0.

## Upgrading from earlier versions (1.4 / 2.0.1)

- CLI: `--strict` is deprecated; use `--strictness strict|lenient` (defaults to strict). The old flag remains temporarily.
- Length markers: legacy `[#N]` headers are no longer produced; decoders stay lenient to legacy files.
- Row depth: list-item tabular arrays emit rows at depth `+2` (v3 layout) while decoders accept legacy depths.
- New optional features: key folding (`keyFolding="safe"`, `flattenDepth`) and path expansion (`expandPaths="safe"`) are off by default for backward compatibility.

## Scala Implementation Architecture

### Pure Functional Design

toon4s implements the TOON spec with pure functional programming principles:

**Pure Functions**: All encode/decode operations are referentially transparent with no side effects. The API returns `Either[DecodeError, JsonValue]` instead of throwing exceptions, enabling composability with Cats, ZIO, and other FP libraries.

**Immutable ADTs**: The `JsonValue` sealed trait provides exhaustive pattern matching over `JNull`, `JBool`, `JNumber`, `JString`, `JArray`, and `JObj`. Objects use `VectorMap` for deterministic field ordering.

**Type Safety**: Scala 3 derivation via `Encoder.derived` and `Decoder.derived` provides compile-time guarantees. Scala 2.13 users get equivalent safety through `ToonTyped` typeclasses.

**Stack Safety**: All recursive operations use tail recursion or trampolining. The visitor pattern and cursor navigation are stack-safe, handling arbitrarily deep structures within configured limits.

### Performance with Purity

toon4s achieves **2x performance improvement** while maintaining functional purity:

**Zero-Allocation Patterns**:
- Pre-allocated `StringBuilder` capacity based on estimated output size
- Single-pass string processing (combined quote-finding + unescaping)
- Cached common patterns (array headers for lengths 0-10)
- `VectorBuilder` with while loops instead of functional chains

**Hot-Path Optimization**:
- Direct character operations instead of string allocations
- Pattern matching for delimiter dispatch
- Early-exit evaluation with `iterator.forall`
- Hoisted constants outside loops

**Memory Efficiency**:
- Streaming visitors with O(depth) memory usage
- No intermediate allocations in visitor chains
- Tail-recursive iteration for large arrays
- Stack-safe cursor navigation

### Visitor Pattern Architecture

The visitor pattern enables zero-overhead transformations:

**Universal TreeWalker**: Adapts external JSON libraries (Jackson, Circe, Play JSON) without converting to intermediate `JsonValue` representation.

**Composable Visitors**: Chain multiple visitors (`FilterKeysVisitor`, `JsonRepairVisitor`, `StringifyVisitor`) in a single pass with O(1) memory overhead.

**Streaming Guarantees**: Process millions of rows with constant memory using `foreachTabular` and `foreachArrays`, which iterate without building full ASTs.

### Type-Driven Development

toon4s leverages Scala's type system for correctness:

**Compile-Time Validation**: Encoder/Decoder derivation catches schema mismatches at compile time, not runtime.

**Sealed ADTs**: Exhaustive pattern matching ensures all `JsonValue` cases are handled, preventing runtime errors.

**Phantom Types**: Configuration types like `Strictness` and `KeyFolding` use sealed traits to restrict valid values at compile time.

**Zero-Cost Abstractions**: Type-level programming and inline optimizations ensure abstraction overhead is eliminated by the compiler.
