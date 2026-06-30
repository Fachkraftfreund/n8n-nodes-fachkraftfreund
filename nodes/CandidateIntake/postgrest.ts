import type { IDataObject, IExecuteFunctions, IHttpRequestOptions } from 'n8n-workflow';
import type { CandidateRow, JobTitleAlias, JobTitleRecord, PostingGroup } from './types';

// ─── PostgREST access layer ──────────────────────────────────────────────────
// Thin wrappers over `httpRequestWithAuthentication` using n8n's built-in
// `supabaseApi` credential (host + service-role key). The credential's
// authenticate injects the `apikey` / `Authorization` headers; we only build
// the `${host}/rest/v1/<table>` URL, query string and body.

export const SUPABASE_CREDENTIAL = 'supabaseApi';

export interface Submission {
	id: string;
	[column: string]: unknown;
}

function restUrl(host: string, table: string): string {
	return `${host.replace(/\/+$/, '')}/rest/v1/${table}`;
}

async function pgRequest<T>(
	ctx: IExecuteFunctions,
	options: IHttpRequestOptions,
): Promise<T> {
	return ctx.helpers.httpRequestWithAuthentication.call(
		ctx,
		SUPABASE_CREDENTIAL,
		{ json: true, ...options },
	) as Promise<T>;
}

const JOB_TITLE_COLUMNS =
	'id,name,short_form,perspective_funnel_id,meta_campaign_id,nationwide_meta_adset_id,nationwide_meta_campaign_id';

const POSTING_GROUP_COLUMNS = 'id,job_id,customer_id,meta_campaign_id,updated_at,deleted_at';

/** All job titles (reference data; small set, fetched once per execution). */
export async function fetchJobTitles(
	ctx: IExecuteFunctions,
	host: string,
): Promise<JobTitleRecord[]> {
	return pgRequest<JobTitleRecord[]>(ctx, {
		method: 'GET',
		url: restUrl(host, 'job_titles'),
		qs: { select: JOB_TITLE_COLUMNS },
	});
}

/** All live job-title aliases (reference data; fetched once per execution). */
export async function fetchJobTitleAliases(
	ctx: IExecuteFunctions,
	host: string,
): Promise<JobTitleAlias[]> {
	return pgRequest<JobTitleAlias[]>(ctx, {
		method: 'GET',
		url: restUrl(host, 'job_title_aliases'),
		qs: { select: 'alias_lower,canonical_name', deleted_at: 'is.null' },
	});
}

/**
 * Live posting groups whose Meta id bag overlaps the given ids.
 * Ordered most-recently-updated first so the caller can pick deterministically.
 */
export async function fetchPostingGroups(
	ctx: IExecuteFunctions,
	host: string,
	metaIds: string[],
): Promise<PostingGroup[]> {
	return pgRequest<PostingGroup[]>(ctx, {
		method: 'GET',
		url: restUrl(host, 'posting_groups'),
		qs: {
			select: POSTING_GROUP_COLUMNS,
			deleted_at: 'is.null',
			meta_campaign_id: `ov.{${metaIds.join(',')}}`,
			order: 'updated_at.desc.nullslast',
		},
	});
}

/** Broad de-dup prefilter: candidates matching the `or=()` clause, soft-deleted excluded. */
export async function fetchCandidatePrefilter(
	ctx: IExecuteFunctions,
	host: string,
	orClause: string,
): Promise<CandidateRow[]> {
	return pgRequest<CandidateRow[]>(ctx, {
		method: 'GET',
		url: restUrl(host, 'candidates'),
		qs: { select: '*', deleted_at: 'is.null', or: orClause },
	});
}

/**
 * Gender self-lookup votes: the `gender` of live candidates whose `first_name`
 * matches `firstName` case-insensitively, restricted to `male`/`female`
 * (soft-deleted excluded). The majority is tallied in TypeScript by the caller.
 */
export async function fetchGenderVotes(
	ctx: IExecuteFunctions,
	host: string,
	firstName: string,
): Promise<string[]> {
	const rows = await pgRequest<{ gender: string | null }[]>(ctx, {
		method: 'GET',
		url: restUrl(host, 'candidates'),
		qs: {
			select: 'gender',
			deleted_at: 'is.null',
			first_name: `ilike.${firstName}`,
			gender: 'in.(male,female)',
		},
	});
	return rows.map((r) => r.gender).filter((g): g is string => g != null);
}

export async function insertCandidate(
	ctx: IExecuteFunctions,
	host: string,
	payload: IDataObject,
): Promise<CandidateRow> {
	const rows = await pgRequest<CandidateRow[]>(ctx, {
		method: 'POST',
		url: restUrl(host, 'candidates'),
		headers: { Prefer: 'return=representation' },
		body: payload,
	});
	return rows[0];
}

export async function patchCandidate(
	ctx: IExecuteFunctions,
	host: string,
	candidateId: string,
	payload: IDataObject,
): Promise<void> {
	await pgRequest<unknown>(ctx, {
		method: 'PATCH',
		url: restUrl(host, 'candidates'),
		qs: { id: `eq.${candidateId}` },
		headers: { Prefer: 'return=minimal' },
		body: payload,
	});
}

/**
 * Upsert a file into a public Supabase Storage bucket at `<bucket>/<objectPath>`.
 * Uses the same `supabaseApi` service-role credential as the REST calls (its
 * authenticate injects the auth headers); `x-upsert: true` overwrites any
 * existing object at the path so re-processing the same candidate replaces it.
 */
export async function uploadToStorage(
	ctx: IExecuteFunctions,
	host: string,
	bucket: string,
	objectPath: string,
	body: Buffer,
	contentType: string,
): Promise<void> {
	await ctx.helpers.httpRequestWithAuthentication.call(ctx, SUPABASE_CREDENTIAL, {
		method: 'POST',
		url: `${host.replace(/\/+$/, '')}/storage/v1/object/${bucket}/${objectPath}`,
		headers: { 'Content-Type': contentType, 'x-upsert': 'true' },
		body,
	});
}

/** Live submissions for a (candidate, posting group) pair. */
export async function fetchLiveSubmissions(
	ctx: IExecuteFunctions,
	host: string,
	candidateId: string,
	postingGroupId: string,
): Promise<Submission[]> {
	return pgRequest<Submission[]>(ctx, {
		method: 'GET',
		url: restUrl(host, 'submissions'),
		qs: {
			select: 'id',
			candidate_id: `eq.${candidateId}`,
			posting_group_id: `eq.${postingGroupId}`,
			deleted_at: 'is.null',
		},
	});
}

export async function insertSubmission(
	ctx: IExecuteFunctions,
	host: string,
	payload: IDataObject,
): Promise<Submission> {
	const rows = await pgRequest<Submission[]>(ctx, {
		method: 'POST',
		url: restUrl(host, 'submissions'),
		headers: { Prefer: 'return=representation' },
		body: payload,
	});
	return rows[0];
}
