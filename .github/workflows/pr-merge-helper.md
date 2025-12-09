# PR Merge Helper Documentation

## Automatically Adding Co-Authors to PR Merges

GitHub does not have a built-in setting to automatically add co-authors when merging PRs. Here are the recommended approaches:

### Option 1: Manual Co-Author Addition (Native GitHub)

When merging a PR on GitHub's web UI:

1. Click "Merge pull request"
2. Click "Edit" on the commit message
3. Add co-author lines:
   ```
   Co-authored-by: Author Name <author@email.com>
   ```
4. Click "Confirm merge"

### Option 2: Browser Extension (RECOMMENDED)

Install **Better GitHub Co-Authors** Chrome extension:
- Link: https://chromewebstore.google.com/detail/better-github-co-authors/nkemoipciaomkemfjbhfbcokpacdofnb
- Adds an "Add Co-authors" button to PR merge UI
- Automatically collects all PR participants
- One-click addition to commit message

### Option 3: gh CLI

```bash
# Template for merging with co-authors
gh pr merge PR_NUMBER --merge --body "$(cat <<EOF
Your commit message here

Co-authored-by: Contributor Name <contributor@email.com>
EOF
)"
```

### Example: Merging PR #42

```bash
gh pr merge 42 --merge --body "$(cat <<EOF
Merge pull request #42 from rorygraves/perf/encode-optimizations

perf: optimize encode performance ~2x improvement

- Primitives: add quoteAndEscape() to avoid intermediate string allocation
- Primitives: replace trim() equality check with Character.isWhitespace()
- Normalize: use VectorBuilder with while loop instead of .map().toVector
- Normalize: use addOne(k, v) instead of tuple allocation for VectorMap
- Normalize: iterate Product iterators directly instead of using .zip()

Benchmark results: 316 ops/ms → 704 ops/ms (+123%)

Co-authored-by: Rory Graves <rory.graves@thetradedesk.com>
EOF
)"
```

### Best Practices

1. **Always add co-authors** when merging contributor PRs
2. **Use the browser extension** for convenience
3. **Include in commit message**:
   - PR number
   - Detailed description
   - Co-authored-by trailer for all contributors
4. **Verify email addresses** match the contributor's GitHub account

### Co-Author Format

```
Co-authored-by: Name <email@example.com>
```

**Requirements:**
- Must be on a separate line
- Must use exact format with `Co-authored-by:`
- Email should match the contributor's verified GitHub email
- Can have multiple co-authors (one per line)

### Benefits

✅ Contributors appear in the commit on GitHub
✅ Contributors get credit in their contribution graph
✅ GitHub shows multiple avatars on the commit
✅ Proper attribution in git history

### References

- [Creating a commit with multiple authors - GitHub Docs](https://docs.github.com/en/pull-requests/committing-changes-to-your-project/creating-and-editing-commits/creating-a-commit-with-multiple-authors)
- [Better GitHub Co-Authors Extension](https://github.com/delucis/better-github-coauthors)
- [How to add multiple authors to a commit in Github](https://gitbetter.substack.com/p/how-to-add-multiple-authors-to-a)
