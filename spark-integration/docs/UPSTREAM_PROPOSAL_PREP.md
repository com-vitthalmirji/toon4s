# Upstream proposal prep

This file tracks the minimum bar before opening any Spark upstream discussion.

## Non-code readiness

- Third-party page entry is live on `spark-website`.
- Compatibility matrix is current for Spark 3.5.x and 4.0.x.
- At least one independent benchmark reproduction exists in this repo.
- At least one public case study exists in `case-studies.md`.

## Technical readiness

- Public API freeze points documented in `API_STABILITY_POLICY.md`.
- Migration notes documented in `MIGRATION_GUIDE.md`.
- CI for `sparkIntegration` is green on main.
- No known data-loss or correctness P0 issues open.

## Proposal package

Prepare one short package before opening discussion:

1. Problem statement and scope.
2. What is external today and why.
3. Measured wins and known loss cases.
4. Compatibility and migration impact.
5. Minimal ask from Spark maintainers.

## Discussion channels

- First step: open a neutral thread on Spark dev channels with the proposal package.
- Keep wording factual and avoid endorsement language.
- Keep ownership clear: this remains an external project unless Spark maintainers decide otherwise.

## Exit criteria

Do not move to upstream code proposal until all readiness checks above are true.
