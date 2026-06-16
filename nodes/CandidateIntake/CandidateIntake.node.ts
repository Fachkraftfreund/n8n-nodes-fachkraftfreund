import {
	NodeOperationError,
	type IDataObject,
	type IExecuteFunctions,
	type INodeExecutionData,
	type INodeType,
	type INodeTypeDescription,
} from 'n8n-workflow';

import {
	composeName,
	identityFrom,
	normalizeEmailForStorage,
	normalizePhoneForDedup,
	toE164,
} from './normalize';
import { findDuplicate } from './dedup';
import { resolveJobTitle } from './resolveJobTitle';
import {
	fetchCandidatePrefilter,
	fetchJobTitleAliases,
	fetchJobTitles,
	fetchLiveSubmissions,
	fetchPostingGroups,
	insertCandidate,
	insertSubmission,
	patchCandidate,
	uploadToStorage,
} from './postgrest';
import {
	detectResumeType,
	isResumeTooLarge,
	MAX_RESUME_BYTES,
	RESUME_BUCKET,
	resumeObjectPath,
	resumePublicUrl,
	shouldUploadResume,
} from './resume';
import {
	DEFAULT_SOURCE,
	IMPORT_SOURCE,
	type CandidateRow,
	type IntakeInput,
	type JobTitleResolution,
	type NormalizedIdentity,
	type OutputSummary,
	type PostingGroup,
} from './types';

// ─── Field definitions ───────────────────────────────────────────────────────

/** The string-valued intake inputs read by the per-item loop (excludes the boolean education_completed). */
type StringInputField = Exclude<keyof IntakeInput, 'education_completed'>;

const INPUT_FIELDS: { name: StringInputField; label: string }[] = [
	{ name: 'firstname', label: 'First Name' },
	{ name: 'lastname', label: 'Last Name' },
	{ name: 'email', label: 'Email' },
	{ name: 'phone', label: 'Phone' },
	{ name: 'city', label: 'City' },
	{ name: 'postal_code', label: 'Postal Code' },
	{ name: 'pay', label: 'Pay' },
	{ name: 'experience', label: 'Experience' },
	{ name: 'qualifications', label: 'Qualifications' },
	{ name: 'situation', label: 'Situation' },
	{ name: 'meta_campaign_id', label: 'Meta Campaign ID' },
	{ name: 'meta_adset_id', label: 'Meta Adset ID' },
	{ name: 'perspective_funnel_id', label: 'Perspective Funnel ID' },
	{ name: 'source', label: 'Source' },
	{ name: 'job', label: 'Job' },
];

function fieldProperty(name: string, label: string): INodeTypeDescription['properties'][number] {
	return {
		displayName: label,
		name,
		type: 'string',
		default: `={{ $json.${name} }}`,
		description: `Mapped to the candidate's "${name}" field. Pre-wired to $json.${name}; override as needed.`,
	};
}

// ─── Pure helpers ──────────────────────────────────────────────────────────────

function trimToNull(value: string | undefined | null): string | null {
	if (value == null) return null;
	const t = String(value).trim();
	return t === '' ? null : t;
}

function isEmptyValue(value: unknown): boolean {
	return (
		value == null ||
		value === '' ||
		(Array.isArray(value) && value.length === 0)
	);
}

function splitQualifications(value: string | undefined | null): string[] | null {
	const t = trimToNull(value);
	if (t === null) return null;
	const parts = t
		.split(';')
		.map((p) => p.trim())
		.filter((p) => p !== '');
	return parts.length > 0 ? parts : null;
}

/** Non-null Meta ids from the input, used for posting-group overlap + title resolution. */
export function metaIdsOf(input: IntakeInput): string[] {
	return [trimToNull(input.meta_campaign_id), trimToNull(input.meta_adset_id)].filter(
		(v): v is string => v !== null,
	);
}

/** Pick the matching posting group deterministically (most recently updated first). */
export function pickPostingGroup(groups: PostingGroup[]): PostingGroup | null {
	const live = groups.filter((g) => g.deleted_at == null);
	return live.length > 0 ? live[0] : null;
}

function quote(value: string): string {
	return `"${value.replace(/(["\\])/g, '\\$1')}"`;
}

/**
 * Broad PostgREST `or=()` prefilter for candidate de-dup. Returns null when no
 * arm is constructible (no email / phone / name) — caller then treats the
 * candidate as brand-new.
 */
export function buildOrClause(input: IntakeInput, name: string): string | null {
	const arms: string[] = [];

	const email = normalizeEmailForStorage(input.email);
	if (email !== null) arms.push(`email.ilike.${quote(email)}`);

	const phoneKey = normalizePhoneForDedup(input.phone);
	if (phoneKey !== null) {
		const suffix = phoneKey.slice(-7);
		arms.push(`phone.ilike.*${suffix}`);
	}

	const trimmedName = trimToNull(name);
	if (trimmedName !== null) {
		const postal = trimToNull(input.postal_code);
		const city = trimToNull(input.city);
		if (postal !== null) {
			arms.push(`and(name.ilike.${quote(trimmedName)},postal_code.eq.${quote(postal)})`);
		} else if (city !== null) {
			arms.push(`and(name.ilike.${quote(trimmedName)},city.ilike.${quote(city)})`);
		}
	}

	return arms.length > 0 ? `(${arms.join(',')})` : null;
}

/** The mapped candidate data columns (excludes provenance constants). */
export function mappedFields(
	input: IntakeInput,
	name: string,
	resolution: JobTitleResolution,
): IDataObject {
	return {
		name,
		email: normalizeEmailForStorage(input.email),
		phone: toE164(input.phone),
		city: trimToNull(input.city),
		postal_code: trimToNull(input.postal_code),
		desired_salary: trimToNull(input.pay),
		work_experience: trimToNull(input.experience),
		current_situation: trimToNull(input.situation),
		additional_qualifications: splitQualifications(input.qualifications),
		funnel_id: trimToNull(input.perspective_funnel_id),
		job_title_id: resolution.jobTitleId,
		// Keep the raw free-text job when the title could not be resolved.
		job_title: resolution.jobTitleName ?? trimToNull(input.job),
		// Completed-education flag: only `true` yields a status; `false`/absent
		// stays null so isEmptyValue drops it from both insert and enrich payloads.
		education_status: input.education_completed === true ? 'completed' : null,
	};
}

/** Full insert payload for a brand-new candidate. */
export function buildInsertPayload(
	input: IntakeInput,
	name: string,
	resolution: JobTitleResolution,
	appliedAt: string,
): IDataObject {
	const mapped = mappedFields(input, name, resolution);
	const payload: IDataObject = {};
	for (const [key, value] of Object.entries(mapped)) {
		if (!isEmptyValue(value)) payload[key] = value;
	}
	payload.import_source = IMPORT_SOURCE;
	payload.source = trimToNull(input.source) ?? DEFAULT_SOURCE;
	payload.applied_at = appliedAt;
	return payload;
}

/** Enrichment payload: incoming values only for columns currently NULL/empty. */
export function buildEnrichPayload(
	input: IntakeInput,
	name: string,
	resolution: JobTitleResolution,
	existing: CandidateRow,
): IDataObject {
	const mapped = mappedFields(input, name, resolution);
	const payload: IDataObject = {};
	for (const [key, value] of Object.entries(mapped)) {
		if (!isEmptyValue(value) && isEmptyValue(existing[key])) {
			payload[key] = value;
		}
	}
	return payload;
}

// ─── Node ─────────────────────────────────────────────────────────────────────

export class CandidateIntake implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Candidate Intake',
		name: 'candidateIntake',
		icon: 'file:candidateIntake.svg',
		group: ['transform'],
		version: 1,
		subtitle: '={{ "Intake → " + ($parameter["email"] || $parameter["phone"] || "candidate") }}',
		description:
			'Attribute, de-duplicate and write an incoming candidate into the cloud Supabase project, creating a preselection when a posting group matches',
		defaults: { name: 'Candidate Intake' },
		inputs: ['main'],
		outputs: ['main'],
		credentials: [{ name: 'supabaseApi', required: true }],
		properties: [
			...INPUT_FIELDS.map((f) => fieldProperty(f.name, f.label)),
			{
				displayName: 'Resume Source URL',
				name: 'resume_source_url',
				type: 'string',
				default: '',
				description:
					'External link to download the resume file from (e.g. a Perspective upload link). Map explicitly per workflow; leave empty to skip resume capture. The downloaded file is stored in the public "resumes" bucket and its public URL written to candidates.resume_url (only when the candidate has no resume yet).',
			},
			{
				displayName: 'Education Completed',
				name: 'education_completed',
				type: 'boolean',
				default: false,
				description:
					'Whether the candidate has a completed vocational education. When true, sets education_status to "completed" (only if currently empty); when false, the column is left untouched.',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const credentials = await this.getCredentials('supabaseApi');
		const host = String(credentials.host ?? '').replace(/\/+$/, '');

		// Reference data — small sets, fetched once per execution.
		const titles = await fetchJobTitles(this, host);
		const aliases = await fetchJobTitleAliases(this, host);

		const out: INodeExecutionData[] = [];

		for (let i = 0; i < items.length; i++) {
			const input: IntakeInput = {};
			for (const f of INPUT_FIELDS) {
				input[f.name] = this.getNodeParameter(f.name, i, '') as string;
			}
			input.education_completed = this.getNodeParameter('education_completed', i, false) as boolean;

			try {
				const name = composeName(input.firstname, input.lastname);
				if (name === '') {
					throw new NodeOperationError(
						this.getNode(),
						'Candidate has no name: both firstname and lastname are empty',
						{ itemIndex: i },
					);
				}

				// 1. Posting-group attribution.
				const metaIds = metaIdsOf(input);
				let group: PostingGroup | null = null;
				if (metaIds.length > 0) {
					group = pickPostingGroup(await fetchPostingGroups(this, host, metaIds));
				}

				// 2. Job-title resolution.
				const resolution = resolveJobTitle({
					groupJobId: group?.job_id ?? null,
					funnelId: trimToNull(input.perspective_funnel_id),
					metaCampaignId: trimToNull(input.meta_campaign_id),
					metaAdsetId: trimToNull(input.meta_adset_id),
					jobText: trimToNull(input.job),
					titles,
					aliases,
				});

				// 3. Candidate de-dup.
				const identity: NormalizedIdentity = identityFrom({
					email: input.email,
					phone: input.phone,
					name,
					postal_code: input.postal_code,
					city: input.city,
				});
				const orClause = buildOrClause(input, name);
				const candidates = orClause
					? await fetchCandidatePrefilter(this, host, orClause)
					: [];
				const duplicate = findDuplicate(identity, candidates);

				let candidateId: string;
				let candidateCreated: boolean;
				const deduped = duplicate !== null;

				if (duplicate) {
					candidateId = duplicate.id;
					candidateCreated = false;
					const enrich = buildEnrichPayload(input, name, resolution, duplicate);
					if (Object.keys(enrich).length > 0) {
						await patchCandidate(this, host, candidateId, enrich);
					}
				} else {
					const payload = buildInsertPayload(input, name, resolution, new Date().toISOString());
					const created = await insertCandidate(this, host, payload);
					candidateId = created.id;
					candidateCreated = true;
				}

				// 4. Submission (preselection) — only with a matched group and no live submission.
				let submissionId: string | null = null;
				let submissionCreated = false;
				if (group) {
					const existing = await fetchLiveSubmissions(this, host, candidateId, group.id);
					if (existing.length > 0) {
						submissionId = existing[0].id;
					} else {
						const submission = await insertSubmission(this, host, {
							candidate_id: candidateId,
							posting_group_id: group.id,
							customer_id: group.customer_id,
						});
						submissionId = submission.id;
						submissionCreated = true;
					}
				}

				// 5. Resume capture — independent of posting-group attribution.
				// Failures here never drop the candidate: they are recorded in the
				// per-item summary instead of thrown, regardless of continueOnFail.
				let resumeUploaded = false;
				let resumeUrl: string | null = null;
				let resumeError: string | null = null;
				const resumeSourceUrl = this.getNodeParameter('resume_source_url', i, '') as string;
				const existingResumeUrl = duplicate ? duplicate.resume_url : null;
				if (shouldUploadResume(resumeSourceUrl, existingResumeUrl)) {
					try {
						const response = (await this.helpers.httpRequest({
							method: 'GET',
							url: resumeSourceUrl,
							encoding: 'arraybuffer',
							returnFullResponse: true,
						})) as { body: unknown; headers?: Record<string, unknown> };
						const headers = response.headers ?? {};
						const buffer = Buffer.isBuffer(response.body)
							? response.body
							: Buffer.from(response.body as ArrayBuffer);

						// Size guard: reject on the declared Content-Length OR the
						// actual downloaded buffer, whichever trips the ~15 MB cap.
						const declared = Number(headers['content-length']);
						const declaredSize = Number.isFinite(declared) ? declared : 0;
						if (isResumeTooLarge(declaredSize) || isResumeTooLarge(buffer.length)) {
							throw new Error(`resume exceeds ${MAX_RESUME_BYTES}-byte limit`);
						}

						const contentType =
							typeof headers['content-type'] === 'string'
								? (headers['content-type'] as string)
								: null;
						const fileType = detectResumeType(contentType, resumeSourceUrl);
						const objectPath = resumeObjectPath(candidateId, fileType.ext);
						await uploadToStorage(this, host, RESUME_BUCKET, objectPath, buffer, fileType.contentType);
						resumeUrl = resumePublicUrl(host, candidateId, fileType.ext);
						await patchCandidate(this, host, candidateId, { resume_url: resumeUrl });
						resumeUploaded = true;
					} catch (error) {
						resumeError = (error as Error).message;
						resumeUrl = null;
						resumeUploaded = false;
					}
				}

				const summary: OutputSummary = {
					candidate_id: candidateId,
					candidate_created: candidateCreated,
					deduped,
					posting_group_id: group?.id ?? null,
					submission_id: submissionId,
					submission_created: submissionCreated,
					job_title_id: resolution.jobTitleId,
					job_title_match_via: resolution.via,
					resume_uploaded: resumeUploaded,
					resume_url: resumeUrl,
					resume_error: resumeError,
				};
				out.push({ json: summary as unknown as IDataObject, pairedItem: { item: i } });
			} catch (error) {
				if (this.continueOnFail()) {
					out.push({
						json: { error: (error as Error).message, ...(input as IDataObject) },
						pairedItem: { item: i },
					});
					continue;
				}
				throw error;
			}
		}

		return [out];
	}
}
