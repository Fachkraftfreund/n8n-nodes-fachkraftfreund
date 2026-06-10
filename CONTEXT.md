# Fachkraftfreund n8n Nodes

n8n community nodes that integrate Fachkraftfreund's recruiting operations with
external services and the `cloud` Supabase project (recruiting CRM / ATS).

## Language

### Recruiting domain (the `cloud` Supabase project)

**Candidate**:
A job seeker. One row in `candidates`. `name` is a single column (no separate
first/last). `source` is the acquisition channel (enum: `meta_ads`, `indeed`,
`stepstone`, `xing`, `website`, `empfehlung`, `indeed_extension`).
_Avoid_: Applicant, lead, Bewerber.

**Job Title**:
A canonical occupation. One row in `job_titles` (e.g. "Pflegefachkraft"). Carries
the Meta campaign/adset ids used to attribute incoming candidates, plus a
Perspective funnel id. Free-text variants resolve to it via `job_title_aliases`.
_Avoid_: Position, role, profession.

**Posting Group**:
The (Customer × Job Title × location) work unit — one row in `posting_groups`.
Holds `meta_campaign_id text[]`, `job_id → job_titles`, `customer_id → customers`.
This is the thing a candidate is attributed to via Meta ids.
_Avoid_: Campaign, posting, ad group.

**Meta id bag** (`posting_groups.meta_campaign_id`):
Despite the column name, this `text[]` holds Meta **campaign OR adset** ids
interchangeably (verified: all 229 distinct values currently stored are in fact
adset ids). Posting-group attribution is therefore a single array-overlap test of
this column against the non-null set `{input.meta_campaign_id, input.meta_adset_id}`
— never assume the column contains only campaign ids.

**Submission**:
A candidate forwarded into a Posting Group's pipeline. One row in `submissions`,
required keys `candidate_id` + `posting_group_id` + `customer_id`. A
**Preselection** is a Submission at `current_stage = 'preselected'` (the default) —
the stage this intake creates.
_Avoid_: Forwarding (that's a later stage), application.

## Intake input contract

The node receives one candidate per item. `source` is passed in directly as a
`candidate_source` enum value (the upstream n8n flow maps form variants such as
`OnePage → website`); there is no `form_type` field on the node. Empty `source`
defaults to `meta_ads`.

## Intake resolution rules

**Posting-group attribution.** Match a live posting group whose Meta id bag
overlaps `{meta_campaign_id, meta_adset_id}` (array overlap). At most one matches
in practice (ids are unique across live groups). A group match drives the
submission and the candidate's job title.

**Job-title resolution** (first hit wins):
1. matched posting group's `job_id`
2. `perspective_funnel_id` → `job_titles.perspective_funnel_id` (compare before `?`)
3. Meta id → `job_titles.meta_campaign_id` / `nationwide_meta_*` bags
4. fuzzy: trigram similarity of the free-text `job` input against
   `job_title_aliases.alias_lower` → name / `job_titles.name` / `short_form`,
   floor **0.45**. Below floor → `job_title_id` NULL, raw text kept in
   `candidates.job_title`.

**Candidate dedup** (a duplicate if ANY arm matches; soft-deleted excluded):
- normalized email (lowercase+trim; Gmail dot/`+tag` collapsed), OR
- normalized phone (bare significant digits), OR
- normalized name AND postal_code (fall back to name AND city when postal_code
  is absent).
On a match: reuse the existing candidate, enrich only NULL/empty columns, never
overwrite non-null values.

**Submission (preselection).** Created only when a posting group matched and no
live submission for that `(candidate_id, posting_group_id)` pair exists yet.
`customer_id` comes from the posting group; `current_stage` left at default
`'preselected'`.

**No posting-group match** still creates/enriches the candidate (with job title
resolved via funnel/meta/fuzzy); it simply produces no submission.

## Field mapping & constants

- `name` = `"{firstname} {lastname}"` (trimmed). `import_source = 'perspective'`.
  `source` from input (enum), default `meta_ads`. `status` default `new`.
- email stored lowercased+trimmed; phone stored normalized to E.164 (`+49…`).
- `pay → desired_salary`, `experience → work_experience`,
  `situation → current_situation`, `qualifications → additional_qualifications[]`
  (split on `;`, trimmed), `perspective_funnel_id → funnel_id`.

## Architecture decisions

- **Intake runs as a node-orchestrated sequence of Supabase PostgREST calls**
  (not a single Postgres RPC). All matching/dedup/insert logic lives in the node's
  TypeScript. Implication: dedup ("candidate already exists", "submission already
  exists") is check-then-insert and must be made race-safe deliberately (DB unique
  constraints / `on_conflict`), since it is not transactional.
