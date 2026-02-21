# Spark website submission pack

This document is the exact checklist and entry text for `apache/spark-website`.

## Target page and file

- Page: `https://spark.apache.org/third-party-projects.html`
- Source file in `spark-website`: `third-party-projects.md`
- Contribution note in that file: lines under `## Adding new projects`

## Proposed entry

Category choice:

- `Applications using Spark`

Entry text:

```md
- [toon4s-spark](https://github.com/com-vitthalmirji/toon4s/tree/main/spark-integration) - TOON encoding for Apache Spark DataFrames and Datasets, optimized for LLM workloads and tabular analytics.
```

## PR checklist

1. Fork `apache/spark-website`.
2. Edit `third-party-projects.md` and add the entry.
3. Run local site build:

   ```bash
   bundle exec jekyll build
   ```

4. Commit both:
   - `third-party-projects.md`
   - generated site output files from build
5. Open PR with:
   - short neutral description
   - link to `https://github.com/com-vitthalmirji/toon4s`
   - confirmation that this is an external third-party project

## Local build prerequisite

`spark-website` currently needs Ruby `>= 3.0.0` in the local environment for `bundle install` and `jekyll build`.

## Trademark and positioning rules

- Use neutral wording, no endorsement language.
- Keep ASF marks as references to Apache Spark only.
- Keep external project ownership clear (maintained in `com-vitthalmirji/toon4s`).

## Maintainer owner

- GitHub org: `com-vitthalmirji`
- Repository: `https://github.com/com-vitthalmirji/toon4s`
