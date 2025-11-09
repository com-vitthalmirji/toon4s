# Changelog

## [0.1.3] - 2025-11-09


### Bug fixes

- auto-tag now triggers release and changelog workflows

- add actions write permission to auto-tag workflow

- auto-tag now triggers release and changelog workflows Merge pull request #16 from vim89/chroe-scaladocs-bug-fix

- use repository_dispatch to trigger release workflows

- changelog triggers on repository_dispatch

- changelog triggers on repository_dispatch

- use env vars in github-script to avoid syntax errors



### Chroe

- updated release notes


## [0.1.2] - 2025-11-09


### Bug fixes

- update github workflows and dependencies

- update github workflows and dependencies Merge pull request #14 from vim89/chroe-scaladocs-bug-fix

- auto-tag paths-ignore and ci duplication issues

- auto-tag paths-ignore and ci duplication issues Merge pull request #15 from vim89/chroe-scaladocs-bug-fix


## [0.1.1] - 2025-11-09


### Bug fixes

- Simplify release.yml



### Chores

- run scalafmt

- Add toonResult

- Add toonResult - Merge pull request #4 from He-Pin/toonResult

- Rewrite ToonResult in Scala

- Rewrite ToonResult in Scala Merge pull request #10 from He-Pin/scalaOnly



### Documentation

- [skip ci] [skip release]

- update CHANGELOG.md [skip ci]

- update CHANGELOG.md [skip ci]

- Update README to reflect JVM support instead of Scala

- update CHANGELOG.md [skip ci]

- update CHANGELOG.md [skip ci]

- Updated README.md and SCALA-TOON-SPECIFICATION.md

- Updated README.md and SCALA-TOON-SPECIFICATION.md Merge pull request #12 from vim89/chroe-scaladocs-bug-fix

- update CHANGELOG.md [skip ci]



### Features

- add workflow_dispatch trigger for manual releases



### Chroe

- updated benchmarks to log in CI for forked repo PR and comment on PRs

- Scaladocs bug-fixes

- Scaladocs bug-fixes Merge pull request #11 from vim89/chroe-scaladocs-bug-fix

- Fix scaladocs issue

- Fix scaladocs issue Merge pull request #13 from vim89/chroe-scaladocs-bug-fix


## [0.1.0] - 2025-11-08


### Docs

- add deep links, quoting table + diagram; CLI: --stats flag



### Documentation

- update CHANGELOG.md [skip ci]

- update CHANGELOG.md [skip ci]

- update CHANGELOG.md [skip ci]

- update CHANGELOG.md [skip ci]

- update CHANGELOG.md [skip ci]

- update CHANGELOG.md [skip ci]

- update CHANGELOG.md [skip ci]

- update CHANGELOG.md [skip ci]



### Features

- feat: initial TOON format implementation.
Implements core TOON (Token-Oriented Object Notation) functionality:
- JSON to TOON encoding with configurable indentation.
- TOON to JSON decoding with full roundtrip support.
CLI tool for file-based encode/decode operations.
- Cross-platform support (Linux, macOS, Windows)
- Cross-Scala version support (3.3.3, 2.13.14)

This is the first public release of toon4s.

- feat: initial TOON format implementation.
Implements core TOON (Token-Oriented Object Notation) functionality:
- JSON to TOON encoding with configurable indentation.
- TOON to JSON decoding with full roundtrip support.
CLI tool for file-based encode/decode operations.
- Cross-platform support (Linux, macOS, Windows)
- Cross-Scala version support (3.3.3, 2.13.14)

This is the first public release of toon4s.

- feat: initial TOON format implementation.
Implements core TOON (Token-Oriented Object Notation) functionality:
- JSON to TOON encoding with configurable indentation.
- TOON to JSON decoding with full roundtrip support.
CLI tool for file-based encode/decode operations.
- Cross-platform support (Linux, macOS, Windows)
- Cross-Scala version support (3.3.3, 2.13.14)

This is the first public release of toon4s.

- feat: initial TOON format implementation.
Implements core TOON (Token-Oriented Object Notation) functionality:
- JSON to TOON encoding with configurable indentation.
- TOON to JSON decoding with full roundtrip support.
CLI tool for file-based encode/decode operations.
- Cross-platform support (Linux, macOS, Windows)
- Cross-Scala version support (3.3.3, 2.13.14)

This is the first public release of toon4s.

- feat: initial TOON format implementation.
Implements core TOON (Token-Oriented Object Notation) functionality:
- JSON to TOON encoding with configurable indentation.
- TOON to JSON decoding with full roundtrip support.
CLI tool for file-based encode/decode operations.
- Cross-platform support (Linux, macOS, Windows)
- Cross-Scala version support (3.3.3, 2.13.14)

This is the first public release of toon4s.

- feat: initial TOON format implementation.
Implements core TOON (Token-Oriented Object Notation) functionality:
- JSON to TOON encoding with configurable indentation.
- TOON to JSON decoding with full roundtrip support.
CLI tool for file-based encode/decode operations.
- Cross-platform support (Linux, macOS, Windows)
- Cross-Scala version support (3.3.3, 2.13.14)

This is the first public release of toon4s.

- feat: add line/column info to decode errors

  Add ErrorLocation case class with line, column, and snippet fields.
  Update DecodeError trait to include optional location parameter.
  Modify Scanner to include location in tab/indentation errors.
  Update Decoders to include location in list item format errors.
  Add ErrorLocationSpec with 7 tests verifying error location reporting.

  Error messages now formatted as: "5:12: Missing colon\n  key value"
  Backward compatible: location defaults to None.

- feat: add line/column info to decode errors

  Add ErrorLocation case class with line, column, and snippet fields.
  Update DecodeError trait to include optional location parameter.
  Modify Scanner to include location in tab/indentation errors.
  Update Decoders to include location in list item format errors.
  Add ErrorLocationSpec with 7 tests verifying error location reporting.

  Error messages now formatted as: "5:12: Missing colon\n  key value"
  Backward compatible: location defaults to None.

- feat: add line/column info to decode errors

  Add ErrorLocation case class with line, column, and snippet fields.
  Update DecodeError trait to include optional location parameter.
  Modify Scanner to include location in tab/indentation errors.
  Update Decoders to include location in list item format errors.
  Add ErrorLocationSpec with 7 tests verifying error location reporting.

  Error messages now formatted as: "5:12: Missing colon\n  key value"
  Backward compatible: location defaults to None.

- feat: add line/column info to decode errors

  Add ErrorLocation case class with line, column, and snippet fields.
  Update DecodeError trait to include optional location parameter.
  Modify Scanner to include location in tab/indentation errors.
  Update Decoders to include location in list item format errors.
  Add ErrorLocationSpec with 7 tests verifying error location reporting.

  Error messages now formatted as: "5:12: Missing colon\n  key value"
  Backward compatible: location defaults to None.

- Built from the ground up with idiomatic Scala, toon4s delivers:

- Built from the ground up with idiomatic Scala, toon4s delivers:

- Built from the ground up with idiomatic Scala, toon4s delivers:

- feat: toon4s v0.1.0 - production-grade TOON for Scala with idiomatic FP architecture

- feat: toon4s v0.1.0 - production-grade TOON for Scala with idiomatic FP architecture

- feat: toon4s v0.1.0 - production-grade TOON for Scala with idiomatic FP architecture

- feat: toon4s v0.1.0 - production-grade TOON for Scala with idiomatic FP architecture

- feat: toon4s v0.1.0 - production-grade TOON for Scala with idiomatic FP architecture

- toon4s: Token-Oriented Object Notation for Scala

- toon4s: Token-Oriented Object Notation for Scala

- toon4s: Token-Oriented Object Notation for Scala

- toon4s: Token-Oriented Object Notation for Scala


