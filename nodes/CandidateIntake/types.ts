// ─── CandidateIntake shared contracts ────────────────────────────────────────
// Domain language: see CONTEXT.md. These types are the seam shared by the pure
// helper modules (normalize, trigram, dedup, resolveJobTitle) and the node's
// execute orchestration.

/** `candidate_source` enum in the cloud project. */
export type CandidateSource =
	| 'meta_ads'
	| 'indeed'
	| 'stepstone'
	| 'xing'
	| 'website'
	| 'empfehlung'
	| 'indeed_extension';

export const CANDIDATE_SOURCES: readonly CandidateSource[] = [
	'meta_ads',
	'indeed',
	'stepstone',
	'xing',
	'website',
	'empfehlung',
	'indeed_extension',
];

export const DEFAULT_SOURCE: CandidateSource = 'meta_ads';
export const IMPORT_SOURCE = 'perspective';
export const FUZZY_FLOOR = 0.45;

/** Raw form fields for one candidate, read from the node parameters. */
export interface IntakeInput {
	firstname?: string;
	lastname?: string;
	email?: string;
	phone?: string;
	city?: string;
	postal_code?: string;
	pay?: string;
	experience?: string;
	qualifications?: string;
	situation?: string;
	meta_campaign_id?: string;
	meta_adset_id?: string;
	perspective_funnel_id?: string;
	source?: string;
	job?: string;
	/** Completed-education flag. `true` → education_status = 'completed'. */
	education_completed?: boolean;
}

/** Subset of a `job_titles` row used for title resolution. */
export interface JobTitleRecord {
	id: string;
	name: string;
	short_form: string | null;
	perspective_funnel_id: string | null;
	/** Meta id bag (campaign or adset ids). */
	meta_campaign_id: string[] | null;
	nationwide_meta_adset_id: string[] | null;
	nationwide_meta_campaign_id: string[] | null;
}

/** A live `job_title_aliases` row: free-text variant → canonical title. */
export interface JobTitleAlias {
	alias_lower: string;
	job_title_id: string;
}

/** Subset of a `posting_groups` row used for attribution. */
export interface PostingGroup {
	id: string;
	job_id: string | null;
	customer_id: string | null;
	/** Despite the name, holds Meta campaign OR adset ids interchangeably. */
	meta_campaign_id: string[] | null;
	updated_at: string | null;
	deleted_at: string | null;
}

/** Subset of a `candidates` row used for de-dup + enrichment. */
export interface CandidateRow {
	id: string;
	name: string | null;
	email: string | null;
	phone: string | null;
	city: string | null;
	postal_code: string | null;
	deleted_at: string | null;
	[column: string]: unknown;
}

export type JobTitleMatchVia = 'group' | 'funnel' | 'meta' | 'fuzzy' | 'none';

export interface JobTitleResolution {
	jobTitleId: string | null;
	jobTitleName: string | null;
	via: JobTitleMatchVia;
}

/**
 * Normalized identity of an incoming candidate, used by the de-dup predicate.
 * Each field is the comparison key (null when the source field was empty).
 */
export interface NormalizedIdentity {
	/** lowercase+trim, Gmail dot/`+tag` collapsed. */
	email: string | null;
	/** bare significant digits (country code / leading zero stripped). */
	phone: string | null;
	/** accent-stripped, lowercased, whitespace-collapsed name. */
	name: string | null;
	postal_code: string | null;
	city: string | null;
}

/** Output summary, one per input item (see PRD §Output item JSON). */
export interface OutputSummary {
	candidate_id: string;
	candidate_created: boolean;
	deduped: boolean;
	posting_group_id: string | null;
	submission_id: string | null;
	submission_created: boolean;
	job_title_id: string | null;
	job_title_match_via: JobTitleMatchVia;
	/** Whether this run downloaded + uploaded a resume to the resumes bucket. */
	resume_uploaded: boolean;
	/** Public Storage URL written to candidates.resume_url, or null if none was uploaded. */
	resume_url: string | null;
	/** Reason the resume step skipped/failed (oversized, dead link, upload error), or null. */
	resume_error: string | null;
}
