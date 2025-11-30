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
