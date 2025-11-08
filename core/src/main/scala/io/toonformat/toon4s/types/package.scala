package io.toonformat.toon4s

/**
 * Type definitions and domain types for compile-time safety.
 *
 * This package provides newtype wrappers around primitives to prevent mixing up different integer
 * types (indent vs depth vs line numbers).
 *
 * ==Cross-version plan==
 *   - Scala 3: Uses opaque types for zero-cost abstractions
 *   - Scala 2.13: Uses AnyVal wrappers for minimal overhead
 *
 * Both approaches provide the same level of type safety at compile time.
 */
package object types {
  // Cross-version type aliases will be defined in version-specific directories
  // See: scala-3/io/toonformat/toon4s/types/DomainTypes.scala
  // See: scala-2.13/io/toonformat/toon4s/types/DomainTypes.scala
}
