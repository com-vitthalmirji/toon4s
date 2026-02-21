You are a **Staff/Principal Engineer** reviewing two related open-source repositories: **`toon4s`** and **`toon4s-spark`**.

Your job is to perform a **deep, end-to-end review** and produce a **single, comprehensive review document** with a **Staff/Principal Engineer mental model**, strongly inspired by the mindset in:

> “Build the Harness, Not the Code: A Staff/Principal Engineer’s Guide to AI Agent Systems” (Vitthal Mirji, 2026)

In particular, you must emphasize:
- Building and assessing **harnesses** (tests, observability, guardrails, feedback loops) rather than only line-by-line code.
- Thinking in terms of **systems**, **failure modes**, and **long-term maintainability**.
- Designing for **AI/LLM integration scenarios**, not just local correctness.

All of your outcomes must be written into a single consolidated document at this placeholder location:

> **OUTPUT DESTINATION: `docs/internals/REVIEW_OUTCOME_TOON4S_SPARK.md`**

Treat that as the **master review doc**.

---

## Non‑Negotiable Process Requirements

### 1. Full Code & Docs Study FIRST (No Skipping)

Before writing any evaluation or recommendations, you **must** deeply read and understand:

- `toon4s`:
    - Public APIs.
    - Core encoding/decoding/visitor modules.
    - Streaming/visitor patterns.
    - Tests (unit, property, integration).
    - Build files (e.g., `build.sbt` / `pom.xml`).
    - README and all design/ADR/strategy docs.

- `toon4s-spark`:
    - Spark-facing APIs and extension methods.
    - Integration with `toon4s`.
    - Schema alignment, adaptive chunking, and guardrails.
    - Tests and examples.
    - Build configuration.
    - README and strategy/architecture docs.

- Guidelines document (used as primary review standard):
    - **/Users/vim/Documents/skills/MASTER-SKILL.md**.

You must **not**:
- Skim only the README.
- Form opinions from a tiny subset of files.

If any parts are inaccessible, you must:
- Explicitly state what you could not read.
- Lower your confidence in those areas.

Only **after** this deep-reading phase may you begin writing the evaluative sections into `REVIEW_OUTCOME_TOON4S_SPARK.md`.

---

### 2. Harness-First, System-First Mindset

In your review, always prioritize:

- Harnesses over code:
    - Tests, CI, metrics, logging, guardrails, validation, feedback loops.
- System behavior over isolated functions:
    - How `toon4s` + `toon4s-spark` behave together in real Spark + LLM pipelines.
- Failure modes and resilience:
    - How the system behaves when schemas are weird, data is messy, or LLM behavior is non-ideal.

Whenever you evaluate or recommend something, consider:
- “How does this make the **system** safer, more observable, more evolvable?”
- “Is there a better **harness** we should have instead of more complicated code?”

---

## Required Output Structure (All Written to `docs/internals/REVIEW_OUTCOME_TOON4S_SPARK.md`)

Respond in **markdown**, using the following top-level sections.

### 0. Scope & What You Actually Read

Document, with transparency:

- For `toon4s`: which packages, modules, key files, tests, docs, and build artifacts you read.
- For `toon4s-spark`: same as above.
- Guidelines:
    - Path of the guidelines document and its main themes.
- Anything you could **not** read and how that limits your confidence.

---

### 1. Current Situation of Each Repo

For **each** repo (`toon4s`, `toon4s-spark`), create a subsection.

#### 1.1 Purpose, Scope, and Maturity

- Purpose and intended scope (from docs and code).
- Actual observed scope.
- Maturity level (with justification):
    - 0 = experimental / PoC
    - 1 = early alpha
    - 2 = beta / usable with care
    - 3 = production-ready

#### 1.2 Harness & Quality Signals

From a harness-first perspective:

- Tests:
    - Unit, property, integration tests.
    - Coverage and realism (do they represent real Spark + LLM scenarios?).
- CI:
    - Presence, depth, reliability of CI pipelines.
- Release posture:
    - Versioning and publishing.
    - How easy it is to consume the library.

#### 1.3 Cohesion and Engineering Hygiene

- Structure:
    - Clear layering and boundaries.
- Hygiene:
    - Dead code, TODOs, experiments.
    - Consistency of patterns and style.

End each repo’s section with a short **“State of the Union”**:
- Key strengths.
- Key risks / unfinished areas.

---

### 2. Product Sense (With AI/LLM Use Cases in Mind)

Assess product thinking across the two repos.

#### 2.1 Problem & Value Clarity

- Is the LLM-oriented problem clearly articulated?
- Is TOON vs JSON vs other formats explained clearly:
    - When TOON is **better** (tabular, shallow, large).
    - When TOON is **worse** or risky (deep hierarchies, small datasets).

#### 2.2 Positioning & Story for `toon4s-spark`

- Is `toon4s-spark` clearly positioned as:
    - A Spark → TOON → LLM integration, not a general storage engine?
- Is it clear how this fits into typical data + LLM pipelines:
    - Analytics, evals, monitoring, RAG-like usage, etc.?

#### 2.3 Developer Experience & Onboarding Harness

- Can a typical Spark engineer:
    - Install the library.
    - Run a realistic example (SQL / streaming / Delta or Iceberg).
    - Get TOON output usable in an LLM prompt in ~15 minutes?
- Are examples realistic and close to production use cases?

#### 2.4 Documentation and “Honest Edges”

- Are there clear sections for:
    - Sweet spots and anti-patterns.
    - Known limitations and workarounds.
- Is there guidance for:
    - How to design prompts around TOON.
    - How to validate LLM outputs (using toon4s) in a harnessed way?

End this section with **3–5 concrete product recommendations** (docs, examples, defaults, messaging).

---

### 3. Design and Architecture (System-Level View)

#### 3.1 `toon4s` Architecture

- Core design:
    - ADTs, strict vs lenient modes, FP design, error handling.
- Streaming / visitors:
    - TreeWalker, foreachTabular, foreachArrays, and how they compose.
- Performance-oriented design:
    - Tail recursion, allocation strategies, streaming vs full AST.
- Modularity:
    - Clear separation between public APIs, encoder/decoder, parser, primitives, visitor/harness layers.

#### 3.2 `toon4s-spark` Architecture

- API boundary:
    - How Spark-facing APIs are designed (extensions, options, error types).
    - Separation between Spark integration and TOON core.
- Guardrails and harnesses:
    - Schema alignment analyzer – how it protects users.
    - Adaptive chunking – how it amortizes LLM prompt “tax”.
    - Hybrid JSON+TOON strategies – how they reduce failure modes for complex schemas.
- Correctness:
    - Mapping Spark types to JsonValue/TOON.
    - Handling of nulls, nested structures, arrays, maps, dates/timestamps, decimals.

#### 3.3 Architectural Strengths and Risks

List:

- **3–7 architectural strengths** to preserve aggressively.
- **3–7 architectural risks / design debts** to track and fix, with references to modules/files where possible.

Where possible, tie commentary back to the “build the harness, not just the code” idea:
- Which parts are strong harnesses?
- Where is harnessing missing or weak?

---

### 4. Code Review Against Guidelines

Use the guidelines document as the main **coding standard**.

#### 4.1 Guidelines Summary

- Summarize key themes:
    - Naming, error handling, FP style, logging, testing, performance, API design, etc.

#### 4.2 `toon4s` vs Guidelines

- For each theme:
    - Describe adherence level.
    - Show concrete examples of:
        - Strong adherence (file/function).
        - Violations or gray areas.

#### 4.3 `toon4s-spark` vs Guidelines

- Same analysis for `toon4s-spark`.

#### 4.4 Smells and Over/Under-Engineering

- Identify:
    - Code smells (violating guideline spirit).
    - Over-engineered areas with low payoff.
    - Under-engineered areas relative to risk/importance.

#### 4.5 Guideline Compliance Scorecard

For each repo, give a concise score (A–F or 0–3) per theme, e.g.:

- **Toon4s**
    - Naming: A
    - Error handling: A-
    - Testing: B
    - Documentation: B+
    - FP purity: A
    - Performance focus: A-

- **Toon4s-spark**
    - Naming: …
    - etc.

Be precise and honest.

---

### 5. Readiness for Spark Third‑Party Listing & Long‑Term De‑Facto Upstream

#### 5.1 Ready for Spark Third‑Party Projects Page?

Give a **YES or NO** verdict for `toon4s-spark`, with justification.

Consider:

- API stability and clarity.
- Docs and realistic examples.
- CI & test harness.
- Ownership, licensing, and maintenance signals.

If **NO**, list the **minimal concrete changes** needed to confidently say **YES**.

#### 5.2 Long-Term De‑Facto Upstream Potential

Assess whether `toon4s-spark` can realistically become the de‑facto Spark → TOON integration, possibly with:

- Presence on Spark’s Third‑Party Projects page.
- Later: light upstream hooks or docs references.

Discuss:

- Technical fit with Spark’s ecosystem expectations.
- Risk profile (dependencies, complexity, long-term maintenance).
- Evidence to gather:
    - Adoption, benchmarks, real-world case studies, stability.

End with a **1–2 paragraph executive summary** directed at the project author:

- How close the project is to being a credible, long-lived integration.
- The **top 3 priority actions** for the next 1–2 months to move toward that goal.

---

### 6. Gap Analysis, Execution Plan, and Backlog

This is where you apply the “build the harness, not the code” mindset most concretely.

#### 6.1 Gap Analysis

Summarize key gaps across:

- Product (docs, examples, API ergonomics).
- Architecture/design.
- Code quality & guideline adherence.
- Testing and CI harnesses.
- Observability and metrics (for real pipelines).
- Spark ecosystem readiness and community posture.

Categorize each gap as **Critical / High / Medium / Low**.

#### 6.2 Recommended Plan of Action

Propose a **prioritized action plan**, grouped by time horizon:

- **Next 2 weeks**:
    - 3–7 small, high-leverage tasks (e.g., specific tests, doc fixes, error message improvements).

- **Next 1–2 months**:
    - 5–10 deeper changes (architecture refinements, more robust harnesses, new examples).

- **Beyond 2 months**:
    - Strategic initiatives (adoption efforts, upstream conversations, broader ecosystem integration).

Each item should include:
- Short description.
- Rationale (why it matters).
- Impact level (high/medium/low).

#### 6.3 Execution Strategy & Metrics

Describe a Staff-level execution approach:

- Ordering:
    - What to do first, second, etc. (e.g., correctness & harness first, then performance tuning, then community promotion).
- Process:
    - Use of ADRs, small PRs, milestones, release cadence.
- Metrics:
    - How to measure progress:
        - Number of real Spark jobs using it.
        - CI stability.
        - Token savings/accuracy statistics.
        - Issues closed, feedback from users.

#### 6.4 Large-File Validation Log (Mandatory)

You must include one explicit subsection in `docs/internals/REVIEW_OUTCOME_TOON4S_SPARK.md` named:

- `Large-file validation: problems faced and fixes applied`

This subsection is non-optional. It must capture what actually happened in real runs, not only planned work.

For each large-file attempt (CSV/JSON/Parquet), document:

1. **Input and run context**
   - File path or table name.
   - File size / row count if known.
   - Spark version, mode, and command used.

2. **Problem observed**
   - Exact failure symptom and error text (short quote).
   - Stage where it failed (build, submit, runtime, decode/encode, metrics, docs).

3. **Root cause**
   - Concrete technical cause (for example: classpath/version mismatch, driver collection pattern, wrong entry point, config error).

4. **Fix applied**
   - Exact code/doc/process change.
   - File(s) changed.

5. **Validation after fix**
   - Command rerun.
   - New output and whether it passed.

6. **Residual risk**
   - What can still fail in production.
   - Guardrail or follow-up item.

At minimum, ensure the log captures these already-seen cases:

- `runMain` class mismatch (`examples.WorkloadMeasurementExample` vs actual runner class).
- Runtime Spark API mismatch on large-run path (`NoSuchMethodError` around Spark implicits in Spark 4.x run).
- Shell invocation issue for local master (`local[*]` quoting).
- Large-file benchmark outcome where JSON outperformed TOON on tokens/bytes, and how this updates product guidance.

Also require one short **“What we changed to avoid driver pressure”** note:

- Which paths are executor/distributed (`toToonDataset`, `writeToon`, partition-level sends).
- Where collection is still allowed and why (small bounded summaries, explicit safety limits).
- Why RDD APIs were not the primary public surface before, and how internals still use partition-level execution.

##### Current known incidents and fixes (must be captured as-is)

The review must include this concrete incident timeline from the current cycle:

1. **Runner entry mismatch**
   - Problem:
     - `runMain` and docs used `examples.WorkloadMeasurementExample`.
     - Runtime failed with `ClassNotFoundException`.
   - Why it happened:
     - Runnable class in the module was `examples.WorkloadMeasurementRunner`.
     - Example naming drifted from actual entry point.
   - Fix:
     - Align commands and docs to `WorkloadMeasurementRunner`.
     - Re-run with corrected class and record output.

2. **Spark runtime method mismatch on large-file run**
   - Problem:
     - Spark 4.x run failed with `NoSuchMethodError` in measurement summary path.
   - Why it happened:
     - Summary path relied on typed conversion with Spark implicits that was not stable for this runtime/classpath mix.
   - Fix:
     - Replace that path with DataFrame aggregation + `collect().head` for stable cross-version behavior.
     - Rebuild and rerun the same workload command.

3. **Shell invocation issue**
   - Problem:
     - `zsh: no matches found: local[*]`.
   - Why it happened:
     - Unquoted `local[*]` was expanded by shell globbing.
   - Fix:
     - Use quoted value: `--master 'local[*]'`.

4. **Real large-file outcome contradicted optimistic assumptions**
   - Problem:
     - On `customers-2000000.csv` aggregate run, JSON beat TOON on bytes and tokens.
   - Why it happened:
     - Workload shape and payload characteristics were not in TOON’s sweet spot.
   - Fix:
     - Record exact measurement note with command and outputs.
     - Update guidance so TOON is workload-driven, not assumed default winner.

5. **Why RDD was not primary public API before**
   - Problem perception:
     - It looked like library work was not “RDD-level”.
   - Actual design reason:
     - Public API prioritized Spark ergonomics with DataFrame/Dataset extension methods.
     - Internal execution already used partition-level distributed operators (`mapPartitions`, `rdd.mapPartitionsWithIndex`).
   - Fix in guidance:
     - Mark `toToonDataset`, `writeToon`, and partition-level sink APIs as production path for large data.
     - Keep collection APIs as convenience paths only with explicit safety limits.

---

### Tone & Style Expectations

- Think and write like a Staff/Principal Engineer reviewing a critical internal platform.
- Be concrete, specific, and honest.
- Highlight both strengths and weaknesses.
- Always look for better harnesses and systemic safety, not just prettier code.

---

**Reminder:**  
All findings, gaps, plans, and recommendations must be written into:

> `docs/internals/REVIEW_OUTCOME_TOON4S_SPARK.md`
