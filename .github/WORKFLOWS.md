# GitHub workflows documentation

This document explains how the GitHub Actions workflows work together in this repository.

## Overview

The workflows follow a **simple, proven, boring** approach with clear separation of concerns:

```
Code Push → CI Tests → Auto Tag → Changelog → Release
```

## Workflows

### 1. CI (`ci.yml`)

**Purpose**: Run tests and checks on every push and pull request

**Triggers**:
- Pushes to `main`
- Pull requests
- Manual via `workflow_dispatch`
- Reusable via `workflow_call` (e.g., invoked by `release.yml`)

**What it does**:
- Quick checks: formatting, compilation, binary compatibility
- Tests: runs on multiple OS (Ubuntu, macOS, Windows) and Scala versions (3.3.3, 2.13.14)
- Scaladoc: builds API docs early to catch broken links/annotations before merging
- Smoke tests: CLI functionality
- Budget gate: token savings verification
- JMH benchmarks: performance tests
- Merge gate: the `All checks pass` job runs unconditionally and fails the workflow if **any** upstream stage is not `success`, so GitHub never shows the merge button while CI is red or skipped

**Skip**: Add `[skip ci]` or `[skip release]` to commit message

---

### 2. Auto tag (`auto-tag.yml`)

**Purpose**: Automatically create version tags based on conventional commits

**Triggers**:
- Push to `main` branch

**What it does**:
1. Analyzes commits since last tag
2. Determines version bump type based on conventional commits
3. Creates and pushes new tag (e.g., `v0.1.0`)
4. Triggers `release.yml` and `changelog.yml` via tag push

**Uses**: [mathieudutour/github-tag-action](https://github.com/mathieudutour/github-tag-action) - proven, reliable

**Conventional commits**:
- `feat:` → MINOR bump (0.1.0 → 0.2.0)
- `fix:` → PATCH bump (0.1.0 → 0.1.1)
- `feat!:`, `fix!:`, or `BREAKING CHANGE:` → MAJOR bump (0.1.0 → 1.0.0)
- `chore:`, `docs:`, `refactor:`, `test:`, etc. → PATCH bump

**Edge cases handled**:
- First tag starts at `v0.1.0`
- No version bump if no relevant commits
- Tag already exists: skips gracefully
- Multiple commits: uses highest bump type
- Concurrent runs: prevented via concurrency group

**Skip**: Add `[skip ci]` or `[skip release]` to commit message

---

### 3. Changelog (`changelog.yml`)

**Purpose**: Generate changelog from conventional commits

**Triggers**:
- Repository dispatch event `release-completed` (sent by `release.yml`)
- Manual via `workflow_dispatch`

**What it does**:
1. Runs AFTER tag is created
2. Uses `git-cliff` to generate/update `CHANGELOG.md`
3. Commits and pushes back to `main` with `[skip ci]`

**Uses**: [git-cliff](https://git-cliff.org/) - conventional commits → changelog

**Edge cases handled**:
- First release: generates full changelog
- No changes: skips commit
- Concurrent runs: prevented via concurrency group
- Skip ci commits: ignored by `cliff.toml` config

**Configuration**: See `cliff.toml` for customization

---

### 4. Release (`release.yml`)

**Purpose**: Publish to Maven Central and create GitHub Release

**Triggers**:
- Tag push (`v*`)
- Repository dispatch event `release-tag-created` (sent by `auto-tag.yml`)
- Manual via `workflow_dispatch`

**What it does**:
1. Checks whether the most recent `ci.yml` run for the tag’s commit succeeded; if not found or red, it automatically reruns the full CI workflow before continuing
2. Publishes to Maven Central using `sbt-ci-release` (runs `+publishSigned` so Scala 3 and Scala 2.13 artifacts are produced)
3. Packages CLI artifacts (zip, tgz)
4. Creates GitHub Release with artifacts and release notes

**Uses**: `sbt-ci-release` 1.11.0+ (includes sbt-dynver, sbt-pgp, sbt-sonatype, sbt-git)

**Required secrets**:
- `PGP_PASSPHRASE`: PGP signing key passphrase
- `PGP_SECRET`: Base64 encoded PGP secret key
- `SONATYPE_USERNAME`: Sonatype Central Portal username
- `SONATYPE_PASSWORD`: Sonatype Central Portal password

**Edge cases handled**:
- Only runs on version tags
- Fails fast if the project version does not match the tag or is a `SNAPSHOT`
- GitHub Release created only for version tags

**Important**: Uses Sonatype Central Portal (legacy OSSRH sunset 2025-06-30)

---

## Workflow synchronization

The workflows are designed to work together **in sequence**:

### Happy path flow:

```
1. Developer opens a PR with "feat: add new feature"
   ↓
2. CI workflow runs on the PR (tests pass) ✓
   ↓
3. PR is merged into main → Auto Tag workflow runs:
   - Detects "feat:" prefix
   - Creates tag v0.2.0
   - Pushes tag
   ↓
4. Tag push triggers the Release workflow:
   - Verifies CI already passed for the tagged commit (reruns full CI automatically if not)
   - Publishes to Maven Central
   - Packages CLI artifacts and creates the GitHub Release
   - Dispatches release-completed
   ↓
5. Changelog workflow receives release-completed:
   - Generates CHANGELOG.md
   - Commits back to main with [skip ci]
```

### Edge case: No version bump

```
1. Developer commits to main with "docs: update README [skip ci]"
   ↓
2. Skipped by all workflows (contains [skip ci])
```

### Edge case: Documentation change

```
1. Developer commits to main with "docs: update README"
   ↓
2. CI workflow runs (tests pass) ✓
   ↓
3. Auto Tag workflow runs:
   - Detects "docs:" prefix
   - Creates tag v0.1.1 (patch bump)
   - Pushes tag
   ↓
4. Release workflow runs, succeeds, and dispatches release-completed
5. Changelog workflow runs and updates the changelog
```

### Edge case: Multiple commits

```
1. Developer pushes 3 commits:
   - "fix: bug 1"
   - "feat: new feature"
   - "chore: cleanup"
   ↓
2. CI workflow runs
   ↓
3. Auto Tag workflow:
   - Analyzes all 3 commits
   - Determines MINOR bump (feat > fix > chore)
   - Creates tag v0.2.0
```

---

## Skip mechanisms

To prevent infinite loops and unnecessary runs:

1. **[skip ci]**: Skips CI and Auto Tag workflows
2. **[skip release]**: Skips CI and Auto Tag workflows
3. **concurrency groups**: Prevent concurrent runs of the same workflow

---

## Manual triggers

All workflows support manual triggers for testing/debugging:

- **CI**: `gh workflow run ci.yml`
- **Auto Tag**: Not needed (use manual tag instead)
- **Changelog**: `gh workflow run changelog.yml`
- **Release**: `gh workflow run release.yml -f tag=v0.1.0`

---

## Testing the workflows

### Test auto tagging:

```bash
# Make a feature commit
git commit -m "feat: test auto tagging"
git push origin main

# Watch workflows
gh run list --workflow=auto-tag.yml
gh run list --workflow=changelog.yml
gh run list --workflow=release.yml
```

### Test changelog only:

```bash
# Trigger manually via workflow_dispatch
gh workflow run changelog.yml
```

### Test release only:

```bash
# Trigger manually with specific tag
gh workflow run release.yml -f tag=v0.1.0
```

---

## Troubleshooting

### Problem: "Snapshot releases must have -SNAPSHOT version number"

**Cause**: Tag format doesn't match expected pattern

**Solution**: Ensure tags start with `v` (e.g., `v0.1.0`, not `0.1.0`)

### Problem: Changelog is empty

**Cause**: No tags exist yet, or `cliff.toml` configuration issue

**Solution**:
1. Check that tags exist: `git tag -l`
2. Run changelog workflow manually to regenerate
3. Verify `cliff.toml` commit parsers

### Problem: Workflows running in infinite loop

**Cause**: Changelog commit doesn't have `[skip ci]`

**Solution**: Verify changelog.yml line 71 has `[skip ci]`

### Problem: Auto tag not creating tag

**Cause**: No conventional commit prefix, or commit message skipped automation

**Solution**:
1. Use conventional commit format
2. Verify commit message doesn't have `[skip ci]` / `[skip release]`

---

## Conventional commits cheat sheet

```bash
# Feature (minor bump)
git commit -m "feat: add new parser"

# Bug fix (patch bump)
git commit -m "fix: resolve encoding issue"

# Breaking change (major bump)
git commit -m "feat!: change API signature"
# or
git commit -m "feat: change API

BREAKING CHANGE: API signature changed"

# Other types (patch bump)
git commit -m "docs: update README"
git commit -m "chore: update dependencies"
git commit -m "refactor: simplify code"
git commit -m "test: add unit tests"
git commit -m "perf: improve performance"

# Skip CI/release
git commit -m "docs: typo fix [skip ci]"
```

---

## Version history

- **v1.0.0**: Complete rewrite using proven GitHub Actions
  - mathieudutour/github-tag-action for auto-tagging
  - sbt-ci-release 1.11.0 for publishing
  - git-cliff for changelog generation
  - Simple, boring, reliable workflows

---

## References

- [Conventional Commits](https://www.conventionalcommits.org/)
- [sbt-ci-release](https://github.com/sbt/sbt-ci-release)
- [git-cliff](https://git-cliff.org/)
- [mathieudutour/github-tag-action](https://github.com/mathieudutour/github-tag-action)
- [Semantic Versioning](https://semver.org/)
