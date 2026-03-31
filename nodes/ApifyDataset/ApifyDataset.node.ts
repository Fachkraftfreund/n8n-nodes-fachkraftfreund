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
				'apifyApi',
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
				'apifyApi',
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
		credentials: [{ name: 'apifyApi', required: true }],
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
						displayName: 'Offset',
						name: 'offset',
						type: 'number',
						default: 0,
						description:
							'Number of raw dataset items to skip (across all datasets). Use with Limit to chunk large result sets across multiple executions.',
						typeOptions: { minValue: 0 },
					},
					{
						displayName: 'Limit',
						name: 'limit',
						type: 'number',
						default: 0,
						description:
							'Maximum number of raw dataset items to fetch (0 = unlimited). Use with Offset to chunk large result sets.',
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
			offset?: number;
			limit?: number;
		};
		const pageSize = options.pageSize ?? ITEMS_PAGE_SIZE;
		const globalOffset = options.offset ?? 0;
		const globalLimit = options.limit ?? 0; // 0 = unlimited

		// Use calendar-date comparison (YYYY-MM-DD) to avoid timezone edge cases
		const cutoffDate = new Date(startDate).toISOString().slice(0, 10);

		const platforms: { param: string; platform: Platform }[] = [
			{ param: 'arbeitsamtActorId', platform: 'arbeitsamt' },
			{ param: 'indeedActorId', platform: 'indeed' },
			{ param: 'stepstoneActorId', platform: 'stepstone' },
		];

		// Step 1: Collect all runs across all platforms in deterministic order
		const datasets: { datasetId: string; platform: Platform }[] = [];

		for (const { param, platform } of platforms) {
			const actorId = this.getNodeParameter(param, 0) as string;
			if (!actorId) continue;
			const runs = await collectRuns(this, actorId, cutoffDate);
			for (const run of runs) {
				datasets.push({ datasetId: run.defaultDatasetId, platform });
			}
		}

		// Step 2: Fetch item counts for all datasets (parallel)
		const itemCounts = await Promise.all(
			datasets.map((ds) => getDatasetItemCount(this, ds.datasetId)),
		);
		const totalItemCount = itemCounts.reduce((a, b) => a + b, 0);

		// Step 3: Fetch only the datasets (or slices) that fall within offset/limit
		const jobsByCompany = new Map<string, IDataObject[]>();
		let cumulative = 0;
		let fetched = 0;
		const effectiveLimit = globalLimit > 0 ? globalLimit : Infinity;

		for (let i = 0; i < datasets.length; i++) {
			if (fetched >= effectiveLimit) break;

			const dsItemCount = itemCounts[i];

			// Skip dataset entirely if it falls before the offset
			if (cumulative + dsItemCount <= globalOffset) {
				cumulative += dsItemCount;
				continue;
			}

			const ds = datasets[i];
			const mapper = MAPPERS[ds.platform];

			// Calculate offset within this dataset and how many items to read
			const dsOffset = Math.max(0, globalOffset - cumulative);
			const remaining = effectiveLimit - fetched;
			const dsLimit = Math.min(remaining, dsItemCount - dsOffset);

			// Fetch items from this dataset with calculated offset/limit
			const dsUrl = `${API_BASE}/datasets/${ds.datasetId}/items`;
			let localOffset = dsOffset;
			let localFetched = 0;

			while (localFetched < dsLimit) {
				const fetchLimit = Math.min(pageSize, dsLimit - localFetched);
				const items = (await this.helpers.httpRequestWithAuthentication.call(
					this,
					'apifyApi',
					{
						method: 'GET',
						url: dsUrl,
						qs: { offset: localOffset, limit: fetchLimit, format: 'json' },
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
					let group = jobsByCompany.get(key);
					if (!group) {
						group = [];
						jobsByCompany.set(key, group);
					}
					group.push(mapped);
				}

				localOffset += items.length;
				localFetched += items.length;
				if (items.length < fetchLimit) break;
			}

			fetched += localFetched;
			cumulative += dsItemCount;
		}

		// Build one output item per company
		const returnData: INodeExecutionData[] = [];
		for (const [normalizedName, jobs] of jobsByCompany) {
			const first = jobs[0];
			returnData.push({
				json: {
					company: {
						name: first.company_name,
						city: first.city ?? null,
						normalized_name: normalizedName,
						normalized_city: normalize(first.city as string | undefined),
					},
					jobs,
					totalItemCount,
				},
			});
		}

		// If no companies found, still output pagination info
		if (returnData.length === 0) {
			returnData.push({
				json: { company: null, jobs: [], totalItemCount },
			});
		}

		return [returnData];
	}
}

// ─── Dataset metadata ────────────────────────────────────────────────────────

async function getDatasetItemCount(
	ctx: IExecuteFunctions,
	datasetId: string,
): Promise<number> {
	const res = (await ctx.helpers.httpRequestWithAuthentication.call(
		ctx,
		'apifyApi',
		{
			method: 'GET',
			url: `${API_BASE}/datasets/${datasetId}`,
			timeout: 10_000,
		},
	)) as { data?: { itemCount?: number } };
	return res?.data?.itemCount ?? 0;
}

// ─── Run collection ──────────────────────────────────────────────────────────

async function collectRuns(
	ctx: IExecuteFunctions,
	actorId: string,
	cutoffDate: string,
): Promise<ApifyRun[]> {
	const matching: ApifyRun[] = [];
	let offset = 0;
	let done = false;

	while (!done) {
		const res = (await ctx.helpers.httpRequestWithAuthentication.call(
			ctx,
			'apifyApi',
			{
				method: 'GET',
				url: `${API_BASE}/acts/${encodeURIComponent(actorId)}/runs`,
				qs: { limit: RUNS_PAGE_SIZE, offset, desc: true },
				timeout: 30_000,
			},
		)) as ApifyListResponse<ApifyRun>;

		const runs = res?.data?.items ?? [];
		if (runs.length === 0) break;

		for (const run of runs) {
			// Compare calendar dates only (YYYY-MM-DD)
			const runDate = run.startedAt.slice(0, 10);

			if (runDate < cutoffDate) {
				done = true;
				break;
			}

			if (!run.defaultDatasetId) continue;
			matching.push(run);
		}

		offset += runs.length;
		if (offset >= (res?.data?.total ?? 0)) break;
	}

	return matching;
}
