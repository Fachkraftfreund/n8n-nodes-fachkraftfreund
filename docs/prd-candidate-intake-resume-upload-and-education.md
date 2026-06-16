# PRD: CandidateIntake — resume upload & completed-education flag

Status: Ready for implementation
Domain language: see [CONTEXT.md](../CONTEXT.md)
Extends: [prd-candidate-intake-node.md](./prd-candidate-intake-node.md) (this feature
brings "Resume/file upload", previously Out of Scope, into scope).

## Problem Statement

Candidates arrive at intake with a resume already attached upstream — Perspective
funnels and other sources expose the file as a download link. Today the
CandidateIntake node ignores it: the resume link is dropped on the floor, so
`candidates.resume_url` is left empty and the team has to chase down and re-attach
CVs by hand. Separately, intake sources often know whether the candidate has a
**completed** vocational education, but there is no way to record that on the
candidate, so `education_status` stays NULL even when the answer is known.

## Solution

Extend the CandidateIntake node with two optional inputs:

1. A **Resume Source URL** — when present, the node downloads the file and uploads it
   to the `cloud` project's public `resumes` Storage bucket, then records the public
   link in `candidates.resume_url`.
2. A **completed-education** boolean — when `true`, the node sets the candidate's
   `education_status` to `completed`; when `false` it leaves the column untouched.

Both behaviours obey the node's existing **enrich-never-overwrite** contract: they
only write when the corresponding column on the resolved candidate is NULL/empty, so
re-submissions and duplicates never clobber a curated resume or education status. A
resume that fails to download or upload never drops the candidate — the failure is
reported in the per-item output instead.

## User Stories

1. As a recruiting operator, I want a candidate's resume fetched from its upstream
   link and stored automatically, so that I don't re-attach CVs by hand.
2. As an operator, I want the stored resume reachable at `candidates.resume_url`, so
   that the rest of the ATS opens it exactly like every existing resume.
3. As an operator, I want the resume stored in the public `resumes` bucket like all
   prior resumes, so that links are directly openable with no signed-URL step.
4. As an operator, I want the resume file named by the candidate's id
   (`<candidate_id>.<ext>`), so that re-processing the same candidate overwrites the
   same object instead of littering the bucket with duplicates.
5. As an operator, I want the resume uploaded only when the candidate currently has
   no resume, so that an existing curated CV is never replaced by a later submission.
6. As an operator, I want a brand-new candidate's resume uploaded as part of intake,
   so that the record is complete from creation.
7. As an operator, I want a duplicate candidate that already has a resume to skip the
   download entirely, so that we don't waste bandwidth re-fetching what we have.
8. As an operator, I want the file's type detected from the response so PDFs and Word
   docs are stored with the right extension and Content-Type, so that downloads open
   correctly.
9. As an operator, I want a missing/dead/oversized resume link to NOT fail the
   candidate, so that one bad CV link never costs us the applicant.
10. As an operator, I want each output item to tell me whether a resume was uploaded,
    its URL, and any resume error, so that I can audit or retry failures downstream.
11. As an operator, I want oversized files (> ~15 MB) rejected with a recorded reason,
    so that a pathological link can't bloat storage or stall the run.
12. As an operator, I want the resume step to run regardless of whether a Posting
    Group matched, so that resume capture is independent of preselection.
13. As an operator, I want to record that a candidate has a completed education, so
    that `education_status` reflects what the intake source knows.
14. As an operator, I want the completed-education flag to set `education_status =
    'completed'` only when true, so that a "no/unknown" answer never fabricates a
    status.
15. As an operator, I want the education flag to follow enrich rules — only written
    when `education_status` is currently empty — so that an already-set status (e.g.
    `in_recognition`) is never overwritten by a coarse boolean.
16. As an operator, I want both new inputs to be optional, so that existing workflows
    that don't supply them keep working unchanged.
17. As a data steward, I want resume capture to reuse the existing `supabaseApi`
    service-role credential, so that no new credential or config is introduced.
18. As a developer, I want the resume decision logic (type detection, path/URL
    construction, upload predicate) in pure functions, so that the riskiest bits are
    unit-tested without HTTP.

## Implementation Decisions

- **New inputs on the existing `CandidateIntake` node:**
  - `resume_source_url` (string, **empty default** — mapped explicitly per workflow,
    NOT pre-wired to `$json.resume_url`, to avoid colliding with the upstream field
    name being unknown). This is the **Resume Source URL** (download-from); distinct
    from the stored **Resume URL** (`candidates.resume_url`). See CONTEXT.md.
  - `education_completed` (boolean, default `false`).
- **Bucket & path:** upload to the existing **public** `resumes` bucket (matches the
  dominant convention of stored `resume_url` values). Deterministic object path
  `<candidate_id>.<ext>`. Use Storage upsert (`x-upsert: true`) so re-uploads replace
  the same object — no version history is kept (latest wins).
- **Stored value:** `candidates.resume_url` = the bucket's public object URL
  (`<host>/storage/v1/object/public/resumes/<candidate_id>.<ext>`).
- **Execution ordering:** the candidate id must exist before upload, so the resume is
  handled as a **separate `PATCH candidates.resume_url`** after INSERT/dedup, for both
  new and duplicate candidates (one uniform code path). Candidate ids stay
  DB-generated (`gen_random_uuid()`); a new candidate therefore costs one extra write
  (INSERT then resume PATCH). Client-side UUID generation was considered and rejected
  in favour of a single uniform resume path.
- **Upload predicate (enrich-never-overwrite):** download + upload + PATCH happen only
  when `resume_source_url` is non-empty **AND** the resolved candidate's `resume_url`
  is currently NULL/empty. A duplicate that already has a resume skips the download
  entirely.
- **Download:** plain unauthenticated GET via `this.helpers.httpRequest`
  (`encoding: 'arraybuffer'`). The source link is assumed public/self-authenticating.
- **Size guard:** reject (skip upload, set `resume_error`) when the file exceeds
  **~15 MB** (checked against `Content-Length` and/or the downloaded buffer). The
  public `resumes` bucket itself enforces no limit; this is a node-side sanity cap.
- **Type detection (priority order):** response `Content-Type` header → extension in
  the source URL path → fallback `application/pdf` / `.pdf`. No hard MIME allowlist:
  an unexpected type is still stored, never hard-rejected.
- **Education mapping:** `education_completed === true` →
  `education_status = 'completed'`; `false` → omit the column from the payload
  entirely. The mapping folds into the existing pure field-mapping step, so it
  automatically inherits insert (new) and enrich-only (duplicate) semantics — it is
  written only when the existing `education_status` is NULL/empty. The enum's other
  values (`none`, `in_training`, `in_recognition`) are intentionally unreachable via
  this node.
- **Resume failure handling:** download/upload/PATCH errors are caught locally and
  recorded; they never throw and never drop the candidate, independent of
  `continueOnFail`. The candidate/submission result stands.
- **Output summary additions:** the per-item JSON gains
  `resume_uploaded: boolean`, `resume_url: string | null`, and
  `resume_error: string | null`. Existing fields are unchanged.
- **Version bump:** MINOR bump of `package.json` `version` in the same commit
  (new feature, per CLAUDE.md).

## Testing Decisions

- **What makes a good test here:** assert externally observable behaviour — given an
  input item and a known DB/HTTP state, the right file is (or isn't) downloaded and
  uploaded to the right path, `resume_url`/`education_status` are written per the
  enrich rule, and the output summary reflects what happened. Do not assert internal
  call order or helper wiring.
- **New pure-function seam — `nodes/CandidateIntake/resume.ts`** (preferred,
  lowest-risk, no HTTP):
  - type/extension selection (`Content-Type` header → URL extension → pdf fallback),
  - storage object path (`<candidate_id>.<ext>`) and public-URL construction,
  - the upload predicate (source URL present AND existing `resume_url` empty).
  Unit-test these table-driven, mirroring the existing pure-helper tests
  (`candidate-intake-normalize.test.ts`, `-trigram`, `-dedup`,
  `-resolve-job-title`).
- **Education mapping:** covered as part of the existing pure field-mapping helper
  (`mappedFields`/enrich builders) — assert `completed` is emitted only on `true` and
  only when the existing status is empty.
- **Highest integration seam — `execute(this: IExecuteFunctions)`:** extend the
  existing fake harness in `candidate-intake-execute.test.ts` to (a) stub
  `helpers.httpRequest` for the resume download and (b) route Storage requests
  (`/storage/v1/object/...`) alongside the current `/rest/v1/...` routing. Add
  branch tests:
  - new candidate + resume → download, upload to `<candidate_id>.<ext>` with
    `x-upsert`, `PATCH resume_url`, output `resume_uploaded: true`;
  - duplicate with existing non-empty `resume_url` → no download, no upload,
    `resume_uploaded: false`;
  - duplicate with empty `resume_url` → download + upload + PATCH;
  - download/upload failure → candidate still resolved, `resume_error` populated,
    `resume_uploaded: false`, no throw;
  - oversized file → skipped with `resume_error`, candidate intact;
  - `education_completed: true` on a new candidate → `education_status: 'completed'`
    in the insert body; on a duplicate with an already-set status → NOT in the PATCH
    body.
- **Prior art:** the existing `candidate-intake-execute.test.ts` fake-`IExecuteFunctions`
  harness (stubbing `getInputData`, `getNodeParameter`, `continueOnFail`,
  `getCredentials`, `helpers.httpRequestWithAuthentication`) and the pure-helper unit
  tests listed above.

## Out of Scope

- Moving resumes to the private `candidate-resumes` bucket or introducing signed
  URLs / access control over candidate resume PII (status quo public bucket retained).
- Resume **version history** — only the latest resume per candidate is kept
  (deterministic path is overwritten).
- Reaching the `none`, `in_training`, or `in_recognition` education statuses (a
  boolean only expresses `completed` vs. leave-empty).
- Parsing/OCR of the resume into `resume_markdown`, `work_experience`, `skills`, etc.
- Backfilling resumes or education status for existing candidate rows.
- Authenticated/credentialed resume downloads (e.g. links behind a private bucket or
  token-gated endpoint).
- Hard MIME allowlisting / virus scanning of uploaded files.

## Further Notes

- `resume_url` is overloaded on purpose in two directions: the **input** is the
  Resume Source URL (download-from), the **column** `candidates.resume_url` is the
  stored Resume URL (Supabase Storage). CONTEXT.md defines both terms to keep them
  apart.
- Storage buckets in the `cloud` project (ref `spjjasgecjgfbdrqpanh`): `resumes`
  (public, no size/MIME limit — the target), `candidate-resumes` (private, 10 MB,
  pdf-only — not used here), `sm-post-media`.
- `education_status` enum labels: `none`, `in_training`, `in_recognition`,
  `completed`. As of design time ~8,400 candidates have it NULL; `completed` is the
  most common set value (~1,150).
- The resume step is independent of Posting-Group attribution and Submission
  creation — it runs whenever a Resume Source URL is supplied and the candidate lacks
  a resume.
