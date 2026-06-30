# PRD: CandidateIntake — first/last name, gender, title

Status: Ready for implementation
Domain language: see [CONTEXT.md](../CONTEXT.md)
Builds on: [prd-candidate-intake-node.md](./prd-candidate-intake-node.md)

## Problem Statement

The `candidates` table now has `first_name`, `last_name`, `gender`, and `title`
(academic title) columns, and the rest of the recruiting CRM/ATS reads them. The
CandidateIntake node — the node that creates candidates from Perspective/Meta
intake — only writes the single composed `name` column. So every candidate it
creates lands with empty `first_name`/`last_name`/`gender`/`title`, and someone (or
some other process) has to fill them in afterwards.

On top of that, intake name data arrives dirty: all-caps (`MAX MUSTERMANN`),
letter-spaced (`M A X  M U S T E R M A N N`), and occasionally prefixed with an
academic title (`Dr. Max`). Today that lands verbatim in `name`, producing ugly,
inconsistent records.

## Solution

Extend CandidateIntake so that, for every candidate it creates or enriches, it:

- cleans each incoming name part (de-spaces letter-spacing, normalizes case to
  `Max Mustermann`, and lifts a `Dr`/`Prof` prefix into `title`);
- writes `first_name` and `last_name` (title-cased, kept consistent with `name`);
- infers `gender` by majority vote over the gender of existing candidates who share
  the same first name, leaving it null when there's no confident answer;
- fills all four new columns through the node's existing enrich-never-overwrite
  rule, so deduped candidates get backfilled only where they're currently empty.

No new form fields, no external services, no DB migration.

## User Stories

1. As a recruiting operator, I want new candidates to have `first_name` and
   `last_name` populated, so that downstream views and exports that rely on those
   columns work without a follow-up backfill.
2. As an operator, I want `first_name`/`last_name` to read consistently with the
   full `name` (same casing), so that a record never shows `MAX` in one place and
   `Max` in another.
3. As an operator, I want all-caps names like `MAX MUSTERMANN` normalized to
   `Max Mustermann`, so that the CRM looks clean and professional.
4. As an operator, I want letter-spaced names like `M A X  M U S T E R M A N N`
   collapsed to `Max Mustermann`, so that mangled form input is repaired on intake.
5. As an operator, I want a genuinely short name like `Jo` left untouched, so that
   the de-spacing repair never mangles real two-letter names.
6. As an operator, I want a multi-part first name like `A N N A  L E N A` restored
   to `Anna Lena`, so that compound given names survive the repair.
7. As an operator, I want German nobiliary particles (`von`, `van`, `de`…) kept
   lowercase when not leading, so that `MAX VON DER BERG` becomes
   `Max von der Berg` and not `Max Von Der Berg`.
8. As an operator, I want a `Dr`/`Prof` prefix moved out of the name into the
   `title` column (normalized to `dr`/`prof`), so that the academic title is stored
   structurally and the name field holds just the name.
9. As an operator, I want unrecognized prefixes left in the name, so that the node
   never silently drops something it doesn't understand.
10. As a recruiter, I want `gender` populated for new candidates, so that I can
    address and segment candidates correctly without manual tagging.
11. As a recruiter, I want gender inferred from the gender we already recorded for
    other candidates with the same first name, so that inference reuses our own
    curated data rather than an external guess.
12. As a recruiter, I want gender decided by the majority of `male`/`female`
    matches, so that a single mislabeled outlier doesn't flip the result.
13. As a recruiter, I want `unknown`-gender rows ignored in the vote, so that a name
    that is merely under-labeled still resolves to its real majority gender.
14. As a recruiter, I want gender left empty when matches tie or there are no
    `male`/`female` matches, so that the node never invents a gender it can't
    support.
15. As a recruiter, I want gender matched case-insensitively on first name, so that
    `ALESSIA`, `Alessia`, and `alessia` all draw on the same evidence.
16. As an operator, I want a deduped (existing) candidate's `first_name`,
    `last_name`, `gender`, and `title` backfilled only when they are currently
    empty, so that newer intake data fills gaps without clobbering curated values.
17. As an operator, I want the gender lookup skipped when gender would not be
    written (e.g. a dedup match that already has a gender), so that intake does no
    wasted database work.
18. As an operator, I want a candidate still created even when no gender can be
    inferred and no title is present, so that incomplete name evidence never blocks
    intake.
19. As a developer, I want the four new columns to flow through the existing
    insert/enrich payload builders, so that their NULL/empty handling matches every
    other field and there is no second code path to maintain.
20. As a developer, I want the name-cleaning logic implemented as small pure
    functions, so that the tricky de-spacing and title-extraction rules are unit
    tested in isolation.

## Implementation Decisions

- **Scope of new columns.** Add `first_name`, `last_name`, `gender`, `title`.
  `gender` is plain `text` (not an enum) with the established convention
  `female` / `male` / `unknown`. `title` is plain `text` holding a normalized
  academic title (`dr` / `prof`, lowercase, no dot).

- **Name cleaning is per-field and pure.** `firstname` and `lastname` arrive as
  separate node inputs, each holding one name part; each is cleaned independently in
  `normalize` before composing `name`. The pipeline per field:
  1. **De-space letters** — a field whose whitespace-split tokens are *all* single
     characters is treated as letter-spaced and collapsed into words, where a run of
     **2+ spaces** is a word boundary (`M A X` → `Max`,
     `A N N A  L E N A` → `Anna Lena`). A field containing any multi-letter token is
     left as-is, so a real two-letter name (`Jo`) is never mangled.
  2. **Normalize case** — lowercase, then title-case reusing the existing
     particle/hyphen rules (so `von`/`van`/`de`… stay lowercase when not leading).
  3. **Extract academic title** — a leading `Dr`/`Prof` token (any case, optional
     trailing dot) is removed and returned as `dr`/`prof`. Only these two are
     recognized; any other leading token stays in the name.

- **Name consistency.** `name` is composed from the cleaned parts, and `first_name`
  / `last_name` store those same cleaned parts, so all three always agree. `title`
  comes from whichever part carried the prefix, with the first name taking
  precedence if both somehow do.

- **Gender by self-lookup (majority of male/female).** Inferred from the cloud
  project's own candidate base: match existing `candidates` whose `first_name`
  equals the incoming first name **case-insensitively**, restricted to
  `gender in (male, female)` and excluding soft-deleted rows, then take the more
  frequent of the two. A tie, or no `male`/`female` match, yields `null`. `unknown`
  rows are excluded by the filter and never counted. This deliberately reuses our
  labeled data instead of an external name→gender API or a bundled dataset.

- **Gender does not live in the pure field mapper.** Because it requires a database
  read, gender is resolved in the node's `execute` orchestration and merged into the
  candidate payload, rather than inside the pure `mappedFields` helper. It is merged
  only when it would actually be written — i.e. for a new candidate, or a dedup
  match whose `gender` is currently NULL/empty — so the lookup is skipped otherwise.

- **Enrich-never-overwrite for all four.** `first_name`, `last_name`, `title`, and
  `gender` go through the existing insert/enrich payload builders: empties are
  dropped on insert, and on a dedup match each column is filled only when currently
  NULL/empty, never overwriting a non-null value. This is the same rule already
  applied to every other candidate field.

- **New access-layer call.** One thin PostgREST read is added for the gender lookup
  (select `gender` from `candidates` filtered by case-insensitive first name,
  `gender in (male,female)`, soft-deleted excluded). The majority tally is computed
  in TypeScript, consistent with the node's other "fetch small set, decide in TS"
  patterns.

- **No external dependency, no migration.** The columns already exist; the feature
  is purely node-side. The intake remains a node-orchestrated sequence of PostgREST
  calls (per the existing architecture decision), and the new gender read is one
  more such call — still check-then-act, not transactional.

- **Out-of-band fillers untouched.** Other processes that currently populate these
  columns (e.g. the resume-upload path) are left as-is; this PRD only makes the
  Perspective/Meta intake fill them at creation time.

- **Version bump.** Bump `package.json` `version` in the same commit (per CLAUDE.md)
  — MINOR bump (new behavior / fields).

## Testing Decisions

- **What makes a good test here:** assert externally observable outcomes — given an
  input field, the cleaning functions return the expected cleaned string + extracted
  title; given a known set of same-name candidates, the gender resolver returns the
  expected `male`/`female`/`null`; and given an input item + known DB state,
  `execute` writes the expected `first_name`/`last_name`/`gender`/`title` into the
  insert/enrich payload. Do not assert internal call order.

- **Pure-function seams (preferred, lowest-risk):** the new name-cleaning helpers
  (`de-space`, title-case normalization, academic-title extraction, and the combined
  per-field cleaner) and the gender majority tally are pure and unit-tested in
  isolation. Table-driven cases must include: `MAX MUSTERMANN` → `Max Mustermann`,
  `M A X  M U S T E R M A N N` → `Max Mustermann`, `A N N A  L E N A` → `Anna Lena`,
  `Jo` → `Jo`, `MAX VON DER BERG` → `Max von der Berg`, `Dr. Max` → `(dr, Max)`,
  `Prof Anna` → `(prof, Anna)`; and gender tallies for clear majority, tie → null,
  unknown-only → null, and no-match → null.

- **Integration seam:** the node's `execute(this: IExecuteFunctions)`, driven with a
  faked `IExecuteFunctions` that stubs `getInputData`, `getNodeParameter`,
  `continueOnFail`, and `helpers.httpRequestWithAuthentication`. Assert on the
  PostgREST request payloads: a new candidate carries the cleaned
  `first_name`/`last_name`/`title` and the looked-up `gender`; a dedup match with an
  existing gender triggers no gender lookup and no gender write; a dedup match with
  empty gender is backfilled.

- **Prior art:** mirror the existing CandidateIntake tests — `candidate-intake-
  normalize.test.ts` for the pure name helpers, `candidate-intake-execute.test.ts`
  for the faked-`IExecuteFunctions` integration cases, and `candidate-intake-
  education.test.ts` for the enrich-only-when-empty pattern.

## Out of Scope

- Backfilling `first_name`/`last_name`/`gender`/`title` on existing candidate rows.
- Changing how the resume-upload path or any other process fills these columns.
- Any new form/node input for gender or title (both are derived, not collected).
- Recognizing academic titles beyond `dr`/`prof`, or salutations (`Herr`/`Frau`).
- External name→gender APIs or bundled name→gender datasets.
- Recovering letter-spacing where words are separated by only single spaces (the
  word boundary is unrecoverable; relies on the 2+ space convention).
- Race-safety of the gender read (it is a non-transactional check-then-act, like the
  rest of the intake).

## Further Notes

- The `cloud` project ref is `spjjasgecjgfbdrqpanh`.
- Empirical grounding at design time: `gender` populated on ~11,417 of ~11,426
  candidate rows (`female` 7136, `male` 2493, `unknown` 1788, null 9) — confirming a
  large, usable self-lookup base. `first_name` present on ~11,417 rows, `last_name`
  on ~11,195. `title` present on only 19 rows (`dr` 17, `prof` 2).
- No DB trigger fills these columns; the intake node and other import paths are
  responsible for them.
- The `name is a single column (no separate first/last)` note in CONTEXT.md has been
  superseded; CONTEXT.md now documents the name-cleaning pipeline and the gender
  self-lookup rule.
