# Release playbook

## Purpose

Keep patch releases regular, safe, and easy to consume for Spark users.

## Patch release flow

1. cut release branch from main
2. run full CI including Spark compatibility jobs
3. prepare release notes from `RELEASE_NOTES_TEMPLATE.md`
4. include migration and compatibility section in notes
5. publish artifact and tag release
6. verify publish and quick smoke usage

## Required release artifacts

- release notes with migration section
- compatibility matrix confirmation
- benchmark report reference for any performance claim change
- claims traceability updates for new public claims

## Versioning rules

- patch for fixes and non breaking additions
- minor for removals after prior deprecation window
- major for compatibility resets

## Release quality gates

- all required CI jobs green
- no unresolved must fix items in review
- docs updated for behavior and migration changes
- one maintainer sign off on release notes

## Post release checks

- Maven Central availability check
- dependency pull test in sample Spark project
- issue tracker watch for regression signals
