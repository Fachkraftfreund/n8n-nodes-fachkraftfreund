# PRD: CandidateIntake n8n node

Status: Ready for implementation
Domain language: see [CONTEXT.md](../CONTEXT.md)

## Problem Statement

Candidates apply through Perspective funnels driven by Meta (Facebook/Instagram)
ad campaigns. Today their data lands in n8n but there is no first-class way to get
a candidate into the `cloud` Supabase project correctly: attributed to the right
**Posting Group**, tagged with the right **Job Title**, de-duplicated against the
~9,600 existing candidates, and (where applicable) forwarded as a **Preselection**.
Without this, the team gets duplicate candidate rows, candidates with no/ wrong job
title, and either missing or duplicated preselections — all of which have to be
cleaned up by hand.

## Solution

A new n8n community node, **CandidateIntake**, that takes one candidate's form
fields per input item and, in a single pass, attributes it to a Posting Group,
resolves its Job Title, de-duplicates the Candidate, creates or enriches the
Candidate, and creates a Preselection Submission when (and only when) one is
warranted. It talks to the `cloud` project over Supabase PostgREST using the
service-role key, and reports per-item what it did.

## User Stories

1. As a recruiting operator, I want incoming Perspective/Meta candidates written
   into the `cloud` project automatically, so that I don't hand-enter them.
2. As an operator, I want a candidate attributed to the correct Posting Group via
   its Meta campaign **or** adset id, so that the Preselection lands in the right
   customer pipeline.
3. As an operator, I want the match to work even though `posting_groups.meta_campaign_id`
   actually stores campaign *and* adset ids interchangeably, so that adset-only
   submissions still attribute correctly.
4. As an operator, when a Posting Group matches, I want the Candidate's Job Title
   taken from that group's `job_id`, so that the Candidate and the group agree.
5. As an operator, when no group matches, I want the Job Title resolved from the
   `perspective_funnel_id`, so that funnel-specific intakes still get the right title.
6. As an operator, when neither group nor funnel resolves the title, I want it
   resolved from the Meta campaign/adset id against the Job Title's id bags.
7. As an operator, when none of the above resolve it, I want the free-text `job`
   input fuzzily matched to an existing Job Title, so that imperfect form text still
   usually lands on a real title.
8. As an operator, I want the fuzzy match to refuse weak guesses (similarity below
   0.45), so that a Candidate is left untitled rather than mis-attributed.
9. As an operator, when the title can't be resolved, I want the raw `job` text kept
   on the Candidate, so that no information is lost and it can be triaged later.
10. As an operator, I want the Candidate de-duplicated by normalized email, so that
    `Max@X.de` and `max@x.de` are the same person.
11. As an operator, I want Gmail dot/`+tag` variants treated as the same email, so
    that `m.a.x+job@gmail.com` matches `max@gmail.com`.
12. As an operator, I want de-dup by phone regardless of formatting (`+49 170…`,
    `0170…`, `+4917…`), so that the same number in different shapes is one person.
13. As an operator, I want de-dup by name + postal code (falling back to name + city
    when postal code is missing), so that the same person without contact-field
    overlap is still caught.
14. As an operator, when a duplicate is found, I want it reused and only its empty
    fields enriched, so that newer form data is captured without clobbering curated
    data.
15. As an operator, I want a brand-new Candidate created when no duplicate exists,
    with all provided fields mapped, so that the record is complete from the start.
16. As an operator, I want a Preselection created for the matched Posting Group, so
    that the candidate enters that pipeline at the `preselected` stage.
17. As an operator, I want a Preselection created only if there isn't already a live
    one for that `(candidate, posting_group)` pair, so that re-submissions don't
    duplicate.
18. As an operator, I want the Candidate still created/enriched even when no Posting
    Group matches, so that we never silently drop an applicant — there is simply no
    Submission in that case.
19. As an operator, I want the new Candidate's `source` to come from the input
    (defaulting to `meta_ads`), so that the upstream flow controls channel
    classification (e.g. `OnePage → website`).
20. As an operator, I want each input field mappable in the node UI but pre-wired to
    `={{ $json.<key> }}`, so that it works out-of-the-box yet stays flexible.
21. As an operator, I want one output item per input item summarizing the outcome
    (candidate id, created vs deduped, posting group, submission id/created, job
    title id and how it was matched), so that I can branch or audit downstream.
22. As an operator, I want a failing item to be reportable rather than aborting the
    whole batch when "Continue on Fail" is on, so that one bad row doesn't sink the run.
23. As a data steward, I want stored emails lowercased and phones normalized to E.164
    (`+49…`), so that newly created rows are clean and ready for WhatsApp/Superchat.
24. As a data steward, I want `import_source = 'perspective'` on these rows, so that
    intake provenance is distinguishable from coda/bullhorn/etc.
25. As a developer, I want the node to use the built-in `supabaseApi` credential, so
    that no bespoke credential has to be configured or shipped.

## Implementation Decisions

- **New node `CandidateIntake`** added to the package (`nodes/CandidateIntake/`),
  registered in `package.json` `n8n.nodes`. Single operation, `inputs: ['main']`,
  `outputs: ['main']`, one output item per input item.
- **Credential:** reuse n8n's built-in `supabaseApi` (host + service-role key);
  all DB access via `this.helpers.httpRequestWithAuthentication` against PostgREST
  (`/rest/v1/...`). Service role is required because every relevant table has RLS
  enabled.
- **Architecture (see ADR-equivalent in CONTEXT.md):** the node orchestrates a
  sequence of PostgREST calls in TypeScript — it is **not** a single Postgres
  transaction/RPC. Consequence: candidate and submission de-dup are check-then-insert
  and therefore not atomic/race-safe. Accepted trade-off for logic visibility and
  zero DB migration. (Optional future hardening: DB unique indexes.)
- **Inputs:** one node property per field, each defaulting to `={{ $json.<key> }}`:
  `firstname, lastname, email, phone, city, postal_code, pay, experience,
  qualifications, situation, meta_campaign_id, meta_adset_id, perspective_funnel_id,
  source, job`. There is no `form_type` input.
- **Posting-group attribution:** select live (`deleted_at is null`) `posting_groups`
  where `meta_campaign_id` (a `text[]` bag holding campaign **or** adset ids) overlaps
  the non-null set `{meta_campaign_id, meta_adset_id}`. Ids are unique across live
  groups; if more than one ever matches, pick deterministically (most recently
  updated).
- **Job-title resolution** (first hit wins):
  1. matched Posting Group's `job_id`;
  2. `perspective_funnel_id` vs `job_titles.perspective_funnel_id` (compare the part
     before `?`, since some stored values carry a `?pageId=…` suffix);
  3. Meta id vs `job_titles.meta_campaign_id` / `nationwide_meta_adset_id` /
     `nationwide_meta_campaign_id` bags;
  4. fuzzy: trigram similarity of normalized `job` (reuse the repo's `cleanJobTitle`
     to strip "(m/w/d)" etc.) against `job_title_aliases.alias_lower → canonical`,
     `job_titles.name`, and `short_form`, computed **in TypeScript** over the small
     title set (~96 titles, ~428 aliases). Floor **0.45**; below floor →
     `job_title_id = NULL` and raw text kept in `candidates.job_title`.
- **Candidate de-dup (two-phase):** one broad PostgREST `or=()` prefilter
  (email `ilike`, phone suffix `ilike.%<last-significant-digits>`, and
  `and(name.ilike.., postal_code.eq..)`), then precise confirmation in TypeScript.
  A row is a duplicate if ANY arm holds: normalized email (lowercase+trim; Gmail
  dot/`+tag` collapsed) OR normalized phone (bare significant digits, country
  code/leading-zero stripped) OR (normalized, accent-stripped name AND postal_code;
  postal_code absent → name AND city). Soft-deleted excluded.
- **On duplicate:** reuse the existing candidate id; `PATCH` only columns that are
  currently NULL/empty with incoming values; never overwrite non-null values.
- **Candidate field mapping:** `name = "{firstname} {lastname}"` (trimmed, collapsed
  whitespace; required — empty name is an error/continue-on-fail case);
  `email` (lowercased), `phone` (E.164 `+49…`), `city`, `postal_code`,
  `pay → desired_salary`, `experience → work_experience`,
  `situation → current_situation`,
  `qualifications → additional_qualifications[]` (split on `;`, trimmed),
  `perspective_funnel_id → funnel_id`, resolved `job_title_id` + `job_title` (name);
  `import_source = 'perspective'`, `source` from input (default `meta_ads`),
  `status` left at default `new`, `applied_at = now()`. `candidates.customer_id`
  left NULL (customer linkage lives on the Submission).
- **Submission (Preselection):** created only when a Posting Group matched and no
  live submission exists for `(candidate_id, posting_group_id)`. Set `candidate_id`,
  `posting_group_id`, `customer_id` (from the group); leave `current_stage` at its
  default `'preselected'`, `job_posting_id`/`job_campaign_id` NULL, and other
  columns at defaults.
- **Output item JSON:** `{ candidate_id, candidate_created, deduped,
  posting_group_id, submission_id, submission_created, job_title_id,
  job_title_match_via }` where `job_title_match_via ∈
  {group, funnel, meta, fuzzy, none}`.
- **Errors:** throw `NodeOperationError` by default; when `this.continueOnFail()`,
  emit an `{ error, ...inputContext }` item (with `pairedItem`) instead of aborting.
- **Version bump:** bump `package.json` `version` in the same commit (per CLAUDE.md)
  — MINOR bump (new node).

## Testing Decisions

- **What makes a good test here:** assert externally observable outcomes — given an
  input item and a known DB state, the correct rows are created/enriched and the
  correct output summary is produced — not internal helper call order.
- **Highest existing seam:** the node's `execute(this: IExecuteFunctions)`. Drive it
  with a faked `IExecuteFunctions` (as the other nodes are structured around) that
  stubs `getInputData`, `getNodeParameter`, `continueOnFail`, and
  `helpers.httpRequestWithAuthentication`. Assert on the emitted
  `INodeExecutionData[][]` and on the request payloads sent to PostgREST.
- **Pure-function seams (preferred, lowest-risk):** extract and unit-test the
  decision-dense helpers in isolation — phone→E.164 normalization, phone/email/name
  normalization for de-dup, Gmail dot/`+tag` collapsing, trigram similarity +
  threshold selection, the job-title precedence resolver, and the de-dup predicate
  evaluator. These need no HTTP and cover the riskiest logic. Mirror the existing
  helper style in `nodes/ApifyDataset` (`normalize`, `cleanPhoneNumber`,
  `cleanJobTitle`).
- **Modules tested:** the normalization/matching helpers (unit), the job-title
  resolver (unit, table-driven over the precedence chain), and `execute` (one
  integration-style test per branch: group match + new candidate + new submission;
  dedup reuse + enrich + existing submission skipped; no group → candidate only;
  fuzzy below floor → untitled; continueOnFail path).
- **Prior art:** existing nodes (`ApifyDataset`, `TeamKpiTracker`) use
  `httpRequestWithAuthentication` and `NodeOperationError`; tests should fake those
  the same way.

## Out of Scope

- Any Postgres RPC/migration to make intake atomic or to add normalized columns /
  unique constraints (explicitly deferred; the REST-orchestration trade-off is
  accepted).
- AI/LLM-based job-title inference (trigram fuzzy only).
- Catching Gmail dot/`+tag` duplicates that share **no** phone and **no**
  name+postal — unreconstructable via the REST prefilter; documented limitation.
- Resume/file upload, geocoding (lat/long), WhatsApp/Superchat enrollment, and any
  later Submission stage transitions beyond `preselected`.
- Backfilling or re-normalizing existing candidate rows.
- Multi-tenant / non-`cloud` Supabase projects.

## Further Notes

- **Key data trap:** despite its name, `posting_groups.meta_campaign_id` is a bag of
  Meta campaign **and** adset ids — verified that all 229 distinct stored values are
  in fact adset ids. Matching must test overlap against both inputs.
- The `cloud` project ref is `spjjasgecjgfbdrqpanh`
  (`https://spjjasgecjgfbdrqpanh.supabase.co`).
- Empirical grounding at design time: 369 posting groups, 96 job titles (12 with a
  unique `perspective_funnel_id`, 35 with `short_form`), 428 live aliases, ~9,600
  candidates with no soft-deletes, phones in ~7,600 distinct formats, 598
  non-lowercased emails.
- `candidate_source` enum: `meta_ads, indeed, stepstone, xing, website, empfehlung,
  indeed_extension`. `submissions.current_stage` default `preselected`.
