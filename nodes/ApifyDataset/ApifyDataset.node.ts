import type {
	IDataObject,
	IExecuteFunctions,
	ILoadOptionsFunctions,
	INodeExecutionData,
	INodePropertyOptions,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';

const API_BASE = 'https://api.apify.com/v2';
const ITEMS_PAGE_SIZE = 10_000;
const RUNS_PAGE_SIZE = 1_000;

// ─── Shared helpers ──────────────────────────────────────────────────────────

function cleanJobTitle(title: string): string {
	return title
		.split(' (m/w/d)')[0]
		.split(' m/w/d')[0]
		.split(' (m/w/x)')[0]
		.split(' m/w/x')[0]
		.replace(/\+\d+/g, '')
		.replace(/\((?![^)]*\/in)[^)]*\)/g, '')
		.replaceAll('"', '')
		.replace(' Teilzeit', '')
		.trim();
}

function normalize(str: string | undefined): string | null {
	if (!str) return null;
	return str
		.normalize('NFD')
		.replace(/[\u0300-\u036f]/g, '')
		.toLowerCase()
		.replace(/\s/, '-')
		.replace(/[^a-z0-9-]/g, '');
}

// ─── Indeed-specific helpers ─────────────────────────────────────────────────

const EMAIL_REGEX = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/;
const PHONE_REGEX =
	/(\+49|0049|\+43|0043|\+41|0041|0)\s*[1-9][0-9\s/\-()]{4,}/g;

function findEmailInText(text: string | undefined): string | null {
	if (!text) return null;
	const cleaned = text
		.replace(/ *\(at\) */g, '@')
		.replace(/ *\[at\] */g, '@');
	const m = EMAIL_REGEX.exec(cleaned);
	if (!m) return null;
	const email = m[0].trim().toLowerCase().replace('\u200B', '');
	return email.length > 0 && email.length < 256 ? email : null;
}

function cleanPhoneNumber(phoneNumber: string): string | null {
	if (!phoneNumber) return null;
	let p = phoneNumber.replace(/\s/g, '');
	p = p.replace(/^\+49/g, '0');
	p = p.replace(/^0049/g, '0');
	p = p.replace(/^\+43/g, '0043');
	p = p.replace(/^0043/g, '0043');
	p = p.replace(/^\+41/g, '0041');
	p = p.replace(/^0041/g, '0041');
	return p;
}

function findPhoneNumberInText(text: string | undefined): string | null {
	if (!text) return null;
	const matches = text.match(PHONE_REGEX);
	if (!matches || matches.length === 0) return null;
	const cleaned = matches
		.map(cleanPhoneNumber)
		.filter((n): n is string => n !== null);
	return cleaned.length > 0 ? cleaned[0] : null;
}

// ─── Platform mappers ────────────────────────────────────────────────────────

function mapArbeitsamt(r: IDataObject): IDataObject | null {
	const rawTitle = r.raw_job_title
		? cleanJobTitle(String(r.raw_job_title))
		: r.from_search_url
			? getJobFromArbeitsamtUrl(String(r.from_search_url))
			: '';
	return {
		raw_job_title: rawTitle,
		normalized_job_title: normalize(rawTitle),
		arbeitsamt_id: r.arbeitsagentur_id ?? null,
		platform_and_id: 'arbeitsamt_' + (r.arbeitsagentur_id ?? ''),
		company_name: r.company_name ?? null,
		email: r.email ?? null,
		phone: r.phone ?? null,
		post_date: r.post_date ?? null,
		city: r.city ?? null,
		street: r.street ?? null,
		website: r.website ?? null,
		postal_code: r.postal_code ?? null,
		country: 'de',
		company_size: r.company_size ?? null,
	};
}

function getJobFromArbeitsamtUrl(url: string): string {
	const parts = url.split('&was=');
	if (parts.length < 2) return '';
	const encoded = parts[1].split('&veroeffentlichtseit=1')[0];
	return decodeURIComponent(encoded);
}

function mapIndeed(r: IDataObject): IDataObject | null {
	if (r.error) return null;
	const rawTitle = cleanJobTitle(String(r.title ?? ''));
	const location = r.location as IDataObject | undefined;
	const emails = r.emails as string[] | undefined;
	const descText = r.descriptionText as string | undefined;
	const companyCeo = r.companyCeo as IDataObject | undefined;
	return {
		raw_job_title: rawTitle,
		normalized_job_title: normalize(rawTitle),
		indeed_id: r.jobKey ?? null,
		platform_and_id: 'indeed_' + (r.jobKey ?? ''),
		company_name: r.companyName || r.source || null,
		email: emails?.[0] ?? findEmailInText(descText),
		phone: findPhoneNumberInText(descText),
		post_date: r.datePublished ?? null,
		country: ((location?.countryCode as string) ?? '').toLowerCase() || null,
		city: location?.city ?? null,
		postal_code: location?.postalCode ?? null,
		street: location?.streetAddress ?? null,
		ceo_name: companyCeo?.name ?? null,
		website: r.corporateWebsite ?? null,
	};
}

function mapStepstone(r: IDataObject): IDataObject | null {
	const rawTitle = cleanJobTitle(String(r.title ?? ''));
	const company = r.company_details as IDataObject | undefined;
	const posting = r.posting_details as IDataObject | undefined;
	const workplace = r.workplace_details as IDataObject | undefined;
	return {
		raw_job_title: rawTitle,
		normalized_job_title: normalize(rawTitle),
		stepstone_id: r.id ?? null,
		platform_and_id: 'stepstone_' + (r.id ?? ''),
		company_name: company?.company_name ?? null,
		post_date: posting?.published_at ?? null,
		city: workplace?.job_location ?? null,
	};
}

function isFilteredCompany(name: string): boolean {
	const lower = name.toLowerCase();
	return (
		// Government / institutional
		lower.includes('bundesagentur') ||
		lower.includes('arbeitsagentur') ||
		lower.includes('jobcenter') ||
		name.includes('Gemeinde') ||
		name.includes('Stadt') ||
		lower.includes('landkreis ') ||
		lower.includes('landesamt') ||
		lower.includes('bezirksamt') ||
		lower.includes('staatsanwaltschaft') ||
		lower.includes('bundeswehr') ||
		name.includes('Verein') ||
		name.includes('Kanton') ||
		name.includes('Universität') ||
		// Staffing / recruitment agencies & hospital chains
		lower.includes('personal') ||
		lower.includes('recruit') ||
		lower.includes('zeitarbeit') ||
		lower.includes('leiharbeit') ||
		lower.includes('randstad') ||
		lower.includes('schön klinik') ||
		lower.includes('asklepios') ||
		lower.includes('helios')
	);
}

type Platform = 'arbeitsamt' | 'indeed' | 'stepstone';
const MAPPERS: Record<Platform, (r: IDataObject) => IDataObject | null> = {
	arbeitsamt: mapArbeitsamt,
	indeed: mapIndeed,
	stepstone: mapStepstone,
};

// ─── Apify API types ─────────────────────────────────────────────────────────

interface ApifyRun {
	id: string;
	actId: string;
	status: string;
	startedAt: string;
	finishedAt: string | null;
	defaultDatasetId: string;
}

interface ApifyListResponse<T> {
	data: { total: number; offset: number; limit: number; items: T[] };
}

interface TaggedRun {
	run: ApifyRun;
	platform: Platform;
}

// ─── Load-options: discover actors from run history ──────────────────────────

async function getActors(
	this: ILoadOptionsFunctions,
): Promise<INodePropertyOptions[]> {
	const seen = new Map<string, string>();
	let offset = 0;

	try {
		while (seen.size < 50) {
			const res = (await this.helpers.httpRequestWithAuthentication.call(
				this,
				'fachkraftfreundApifyApi',
				{
					method: 'GET',
					url: `${API_BASE}/actor-runs`,
					qs: { limit: RUNS_PAGE_SIZE, offset, desc: true },
					timeout: 15_000,
				},
			)) as ApifyListResponse<ApifyRun>;

			const items = res?.data?.items ?? [];
			if (items.length === 0) break;
			for (const r of items) {
				if (!seen.has(r.actId)) seen.set(r.actId, r.actId);
			}
			offset += items.length;
			if (offset >= (res?.data?.total ?? 0)) break;
		}
	} catch {
		return [];
	}

	const entries = [...seen.keys()];
	const settled = await Promise.allSettled(
		entries.map(async (actId) => {
			const act = (await this.helpers.httpRequestWithAuthentication.call(
				this,
				'fachkraftfreundApifyApi',
				{
					method: 'GET',
					url: `${API_BASE}/acts/${actId}`,
					timeout: 10_000,
				},
			)) as { data?: { name?: string; title?: string; username?: string } };
			const d = act?.data;
			const label = d?.title || d?.name || actId;
			const owner = d?.username ? ` (${d.username})` : '';
			return { name: `${label}${owner}`, value: actId };
		}),
	);

	const options: INodePropertyOptions[] = [];
	for (const r of settled) {
		if (r.status === 'fulfilled') options.push(r.value);
	}
	options.sort((a, b) => a.name.localeCompare(b.name));
	return options;
}

// ─── Node definition ─────────────────────────────────────────────────────────

function actorProperty(
	displayName: string,
	name: string,
	description: string,
): INodeTypeDescription['properties'][number] {
	return {
		displayName,
		name,
		type: 'options' as const,
		required: true,
		default: '',
		description,
		typeOptions: { loadOptionsMethod: 'getActors' },
		noDataExpression: true,
	};
}

export class ApifyDataset implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Apify Dataset',
		name: 'apifyDataset',
		icon: 'file:apify.svg',
		group: ['transform'],
		version: 1,
		subtitle: '={{ "Runs since " + $parameter["startDate"] }}',
		description:
			'Fetch and map dataset items from Arbeitsamt, Indeed and Stepstone actor runs',
		defaults: { name: 'Apify Dataset' },
		inputs: ['main'],
		outputs: ['main'],
		credentials: [{ name: 'fachkraftfreundApifyApi', required: true }],
		properties: [
			actorProperty(
				'Arbeitsamt Actor',
				'arbeitsamtActorId',
				'Apify actor for Arbeitsamt scraping',
			),
			actorProperty(
				'Indeed Actor',
				'indeedActorId',
				'Apify actor for Indeed scraping',
			),
			actorProperty(
				'Stepstone Actor',
				'stepstoneActorId',
				'Apify actor for Stepstone scraping',
			),
			{
				displayName: 'Runs Started After',
				name: 'startDate',
				type: 'dateTime',
				required: true,
				default: '',
				description:
					'Only include runs that started on or after this date (compared by calendar date, timezone-safe)',
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'Items Page Size',
						name: 'pageSize',
						type: 'number',
						default: ITEMS_PAGE_SIZE,
						description:
							'Number of dataset items fetched per API request',
						typeOptions: { minValue: 1, maxValue: 999_999 },
					},
					{
						displayName: 'Return Single Item',
						name: 'returnSingleItem',
						type: 'boolean',
						default: false,
						description:
							'Whether to return all companies in a single item instead of one item per company',
					},
					{
						displayName: 'Runs Per Batch',
						name: 'runsPerBatch',
						type: 'number',
						default: 0,
						description:
							'Process this many runs at a time before checking the company limit. 0 = process all runs at once.',
						typeOptions: { minValue: 0 },
					},
					{
						displayName: 'Company Limit',
						name: 'companyLimit',
						type: 'number',
						default: 0,
						description:
							'Stop after collecting this many unique companies. 0 = no limit (collect all).',
						typeOptions: { minValue: 0 },
					},
					{
						displayName: 'Max Runs',
						name: 'maxRuns',
						type: 'number',
						default: 0,
						description:
							'Maximum total runs to process across all platforms. 0 = no limit.',
						typeOptions: { minValue: 0 },
					},
				],
			},
		],
	};

	methods = { loadOptions: { getActors } };

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const startDate = this.getNodeParameter('startDate', 0) as string;
		const options = this.getNodeParameter('options', 0, {}) as {
			pageSize?: number;
			returnSingleItem?: boolean;
			runsPerBatch?: number;
			companyLimit?: number;
			maxRuns?: number;
		};
		const pageSize = options.pageSize ?? ITEMS_PAGE_SIZE;
		const returnSingleItem = options.returnSingleItem ?? false;
		const runsPerBatch = options.runsPerBatch ?? 0;
		const companyLimit = options.companyLimit ?? 0;
		const maxRuns = options.maxRuns ?? 0;

		// Normalise to a full UTC ISO string so the comparison is precise
		// down to the second, not just the calendar date.
		const cutoff = new Date(startDate).toISOString();

		const platforms: { param: string; platform: Platform }[] = [
			{ param: 'arbeitsamtActorId', platform: 'arbeitsamt' },
			{ param: 'indeedActorId', platform: 'indeed' },
			{ param: 'stepstoneActorId', platform: 'stepstone' },
		];

		// Phase 1: Collect run metadata for all platforms (lightweight)
		const allRuns: TaggedRun[] = [];
		for (const { param, platform } of platforms) {
			const actorId = this.getNodeParameter(param, 0) as string;
			if (!actorId) continue;
			const runs = await collectRuns(this, actorId, cutoff);
			for (const run of runs) {
				allRuns.push({ run, platform });
			}
		}

		// Sort by startedAt descending so freshest runs are processed first
		allRuns.sort((a, b) => b.run.startedAt.localeCompare(a.run.startedAt));

		// Apply maxRuns cap
		const runsToProcess = maxRuns > 0 ? allRuns.slice(0, maxRuns) : allRuns;

		// Phase 2: Process runs in batches
		const jobsByCompany = new Map<string, IDataObject[]>();
		const filteredKeys = new Set<string>();
		const batchSize = runsPerBatch > 0 ? runsPerBatch : runsToProcess.length;

		for (let batchStart = 0; batchStart < runsToProcess.length; batchStart += batchSize) {
			const batch = runsToProcess.slice(batchStart, batchStart + batchSize);

			for (const { run, platform } of batch) {
				const mapper = MAPPERS[platform];
				const dsUrl = `${API_BASE}/datasets/${run.defaultDatasetId}/items`;
				let offset = 0;

				while (true) {
					const items = (await this.helpers.httpRequestWithAuthentication.call(
						this,
						'fachkraftfreundApifyApi',
						{
							method: 'GET',
							url: dsUrl,
							qs: { offset, limit: pageSize, format: 'json' },
							timeout: 120_000,
						},
					)) as IDataObject[];

					if (!Array.isArray(items) || items.length === 0) break;

					for (const raw of items) {
						const mapped = mapper(raw);
						if (mapped === null) continue;

						const name = mapped.company_name as string | undefined;
						if (!name) continue;

						const key = normalize(name) ?? '';

						// Skip companies that were already filtered out
						if (filteredKeys.has(key)) continue;

						// Filter on first encounter, cache the result
						if (!jobsByCompany.has(key) && isFilteredCompany(name)) {
							filteredKeys.add(key);
							continue;
						}

						let group = jobsByCompany.get(key);
						if (!group) {
							group = [];
							jobsByCompany.set(key, group);
						}
						group.push(mapped);
					}

					offset += items.length;
					if (items.length < pageSize) break;
				}
			}

			// Check company limit after each batch
			if (companyLimit > 0 && jobsByCompany.size >= companyLimit) {
				break;
			}
		}

		// Phase 3: Build output — one item per company, or all in a single item
		const companies: IDataObject[] = [];
		for (const [normalizedName, jobs] of jobsByCompany) {
			const first = jobs[0];
			companies.push({
				company: {
					name: first.company_name,
					city: first.city ?? null,
					normalized_name: normalizedName,
					normalized_city: normalize(first.city as string | undefined),
				},
				jobs,
			});
		}
		jobsByCompany.clear();

		if (returnSingleItem) {
			return [[{ json: { companies } }]];
		}

		const returnData: INodeExecutionData[] = companies.map((c) => ({ json: c }));
		return [returnData];
	}
}

// ─── Run collection ──────────────────────────────────────────────────────────

async function collectRuns(
	ctx: IExecuteFunctions,
	actorId: string,
	cutoff: string,
): Promise<ApifyRun[]> {
	const matching: ApifyRun[] = [];
	let offset = 0;

	while (true) {
		const res = (await ctx.helpers.httpRequestWithAuthentication.call(
			ctx,
			'fachkraftfreundApifyApi',
			{
				method: 'GET',
				url: `${API_BASE}/acts/${encodeURIComponent(actorId)}/runs`,
				qs: { limit: RUNS_PAGE_SIZE, offset, desc: true },
				timeout: 30_000,
			},
		)) as ApifyListResponse<ApifyRun>;

		const runs = res?.data?.items ?? [];
		if (runs.length === 0) break;

		let anyMatchOnPage = false;
		for (const run of runs) {
			if (!run.defaultDatasetId) continue;
			if (run.startedAt >= cutoff) {
				matching.push(run);
				anyMatchOnPage = true;
			}
		}

		offset += runs.length;
		if (offset >= (res?.data?.total ?? 0)) break;
		// If no run on this entire page matched, all remaining (older)
		// runs are almost certainly before the cutoff — stop paginating.
		if (!anyMatchOnPage) break;
	}

	return matching;
}
