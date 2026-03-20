import {
	IExecuteFunctions,
	ILoadOptionsFunctions,
	INodeExecutionData,
	INodePropertyOptions,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';

// ═══════════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════════

interface FetchResult {
	html: string;
	finalUrl: string;
}

interface ScrapeJob {
	itemIndex: number;
	inputUrl: string;
	normalizedUrl: string;
	homepageHtml?: string;
	homepageFinalUrl?: string;
	impressumUrl?: string | null;
	impressumHtml?: string;
	error?: string;
}

const COMMON_IMPRESSUM_PATHS = [
	'/impressum', '/impressum/', '/impressum.html',
	'/impressum.php', '/imprint', '/imprint/',
];

const HTTP_CONCURRENCY = 10;

const USER_AGENT =
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

// ═══════════════════════════════════════════════════════════════════════════════
// Node Definition
// ═══════════════════════════════════════════════════════════════════════════════

export class ImpressumScraper implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Impressum Scraper',
		name: 'impressumScraper',
		icon: 'file:impressum.svg',
		group: ['transform'],
		version: 1,
		subtitle: 'Scrape Impressum data from websites',
		description:
			'Crawls websites to find Impressum pages and extracts structured contact/legal data. Uses direct HTTP; optional Apify fallback for resistant sites.',
		defaults: {
			name: 'Impressum Scraper',
		},
		inputs: ['main'] as const,
		outputs: ['main'] as const,
		credentials: [
			{
				name: 'apifyApi',
				required: false,
			},
			{
				name: 'openAiApi',
				required: false,
			},
		],
		properties: [
			{
				displayName: 'URL',
				name: 'url',
				type: 'string',
				default: '',
				required: true,
				description:
					'The website URL to scrape for Impressum data. Can be a homepage — the node will automatically find the Impressum page.',
				placeholder: 'https://example.de',
			},
			{
				displayName: 'OpenAI Model',
				name: 'openAiModel',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getOpenAiModels',
				},
				default: 'gpt-4.1-nano',
				noDataExpression: true,
				description: 'The OpenAI model to use for enrichment and plausibility re-parsing. Only used when OpenAI credentials are configured.',
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'Timeout (Seconds)',
						name: 'timeout',
						type: 'number',
						default: 15,
						description: 'Timeout for direct HTTP requests in seconds',
					},
					{
						displayName: 'Try Common Paths',
						name: 'tryCommonPaths',
						type: 'boolean',
						default: true,
						description:
							'Whether to try common Impressum URL paths (/impressum, /imprint, etc.) if no link is found in the HTML',
					},
					{
						displayName: 'Check Homepage for Impressum',
						name: 'checkHomepage',
						type: 'boolean',
						default: true,
						description:
							'Whether to check if the homepage itself contains Impressum content',
					},
				],
			},
		],
	};

	methods = {
		loadOptions: {
			async getOpenAiModels(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
				const FALLBACK: INodePropertyOptions[] = [
					{ name: 'gpt-4.1-nano', value: 'gpt-4.1-nano' },
					{ name: 'gpt-4.1-mini', value: 'gpt-4.1-mini' },
					{ name: 'gpt-4.1', value: 'gpt-4.1' },
					{ name: 'gpt-4o-mini', value: 'gpt-4o-mini' },
					{ name: 'gpt-4o', value: 'gpt-4o' },
					{ name: 'o3-mini', value: 'o3-mini' },
					{ name: 'o4-mini', value: 'o4-mini' },
				];

				let apiKey: string;
				try {
					const creds = await this.getCredentials('openAiApi');
					apiKey = creds.apiKey as string;
				} catch {
					return FALLBACK;
				}

				try {
					const response = await this.helpers.httpRequest({
						method: 'GET',
						url: 'https://api.openai.com/v1/models',
						headers: { Authorization: `Bearer ${apiKey}` },
						timeout: 10000,
					});

					const EXCLUDE =
						/audio|image|realtime|tts|transcribe|instruct|search|codex|computer|embedding|moderation|dall-e|sora|whisper|babbage|davinci|chatgpt/i;
					const SKIP_VARIANT = /\d{4}-\d{2}-\d{2}|-\d{3,4}(-|$)|-preview|-16k|-chat-latest/;

					const models: INodePropertyOptions[] = (response?.data || [])
						.map((m: { id: string }) => m.id)
						.filter((id: string) => {
							if (EXCLUDE.test(id)) return false;
							if (SKIP_VARIANT.test(id)) return false;
							return /^(gpt-|o[134])/.test(id);
						})
						.sort((a: string, b: string) => a.localeCompare(b))
						.map((id: string) => ({ name: id, value: id }));

					return models.length > 0 ? models : FALLBACK;
				} catch {
					return FALLBACK;
				}
			},
		},
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		// ── Configuration ───────────────────────────────────────────
		let apifyToken: string | undefined;
		try {
			const creds = await this.getCredentials('apifyApi');
			apifyToken = creds.apiToken as string;
		} catch { /* not configured */ }

		let openAiKey: string | undefined;
		try {
			const creds = await this.getCredentials('openAiApi');
			openAiKey = creds.apiKey as string;
		} catch { /* not configured */ }

		const openAiModel = this.getNodeParameter('openAiModel', 0, 'gpt-4.1-nano') as string;

		const options = this.getNodeParameter('options', 0, {}) as {
			timeout?: number;
			tryCommonPaths?: boolean;
			checkHomepage?: boolean;
		};
		const timeout = (options.timeout ?? 15) * 1000;
		const tryCommonPaths = options.tryCommonPaths !== false;
		const checkHomepage = options.checkHomepage !== false;

		// ── Initialize jobs ─────────────────────────────────────────
		const jobs: ScrapeJob[] = [];
		for (let i = 0; i < items.length; i++) {
			const url = this.getNodeParameter('url', i) as string;
			let normalizedUrl = url.trim();
			if (!/^https?:\/\//i.test(normalizedUrl)) {
				normalizedUrl = 'https://' + normalizedUrl;
			}
			try {
				new URL(normalizedUrl);
			} catch {
				jobs.push({ itemIndex: i, inputUrl: url, normalizedUrl, error: `Invalid URL: ${url}` });
				continue;
			}
			jobs.push({ itemIndex: i, inputUrl: url, normalizedUrl });
		}

		const validJobs = jobs.filter((j) => !j.error);

		// ── Phase 1: Fetch all homepages (parallel HTTP + Apify fallback) ──
		if (validJobs.length > 0) {
			const urls = validJobs.map((j) => j.normalizedUrl);
			const results = await fetchMany(this, urls, timeout, apifyToken);
			for (const job of validJobs) {
				const result = results.get(job.normalizedUrl);
				if (result) {
					job.homepageHtml = result.html;
					job.homepageFinalUrl = result.finalUrl;
				} else {
					job.error = `Failed to fetch homepage: ${job.normalizedUrl}`;
				}
			}
		}

		// ── Phase 2: Find impressum URLs from homepage HTML ─────────
		const jobsWithHomepage = validJobs.filter((j) => j.homepageHtml && !j.error);

		for (const job of jobsWithHomepage) {
			const baseUrl = job.homepageFinalUrl || job.normalizedUrl;
			job.impressumUrl = findImpressumUrl(job.homepageHtml!, baseUrl);

			// Check if homepage itself is the impressum (free — no fetch needed)
			if (!job.impressumUrl && checkHomepage) {
				const text = htmlToText(job.homepageHtml!);
				if (looksLikeImpressum(text)) {
					job.impressumUrl = baseUrl;
					job.impressumHtml = job.homepageHtml;
				}
			}
		}

		// ── Phase 3: Try common paths (batch fetch all candidates) ──
		if (tryCommonPaths) {
			const jobsNeedingPaths = jobsWithHomepage.filter((j) => !j.impressumUrl);

			if (jobsNeedingPaths.length > 0) {
				const candidates: Array<{ url: string; jobIdx: number }> = [];
				for (let ji = 0; ji < jobsNeedingPaths.length; ji++) {
					const base = new URL(
						jobsNeedingPaths[ji].homepageFinalUrl || jobsNeedingPaths[ji].normalizedUrl,
					);
					for (const path of COMMON_IMPRESSUM_PATHS) {
						candidates.push({ url: new URL(path, base).href, jobIdx: ji });
					}
				}

				const uniqueUrls = [...new Set(candidates.map((c) => c.url))];
				const candidateResults = await fetchMany(this, uniqueUrls, timeout, apifyToken);

				for (const { url, jobIdx } of candidates) {
					const job = jobsNeedingPaths[jobIdx];
					if (job.impressumUrl) continue;

					const result = candidateResults.get(url);
					if (result && result.html.length > 500) {
						const text = htmlToText(result.html);
						if (looksLikeImpressum(text)) {
							job.impressumUrl = url;
							job.impressumHtml = result.html;
						}
					}
				}
			}
		}

		// ── Phase 4: Fetch impressum pages not yet fetched ──────────
		for (const job of jobsWithHomepage) {
			if (
				job.impressumUrl &&
				!job.impressumHtml &&
				(job.impressumUrl === job.homepageFinalUrl ||
					job.impressumUrl === job.normalizedUrl)
			) {
				job.impressumHtml = job.homepageHtml;
			}
		}

		const jobsStillNeedingFetch = jobsWithHomepage.filter(
			(j) => j.impressumUrl && !j.impressumHtml,
		);

		if (jobsStillNeedingFetch.length > 0) {
			const urls = jobsStillNeedingFetch.map((j) => j.impressumUrl!);
			const results = await fetchMany(this, urls, timeout, apifyToken);
			for (const job of jobsStillNeedingFetch) {
				const result = results.get(job.impressumUrl!);
				if (result) {
					job.impressumHtml = result.html;
				} else {
					job.error = `Failed to fetch impressum page: ${job.impressumUrl}`;
				}
			}
		}

		// ── Phase 5: Parse results ──────────────────────────────────
		const successfulJobs: Array<{ job: ScrapeJob; data: ImpressumResult; text: string }> = [];

		for (const job of jobs) {
			if (job.error) {
				returnData.push({
					json: { sourceUrl: job.inputUrl, error: job.error, success: false },
					pairedItem: { item: job.itemIndex },
				});
				continue;
			}

			if (!job.impressumUrl || !job.impressumHtml) {
				returnData.push({
					json: { sourceUrl: job.inputUrl, error: `No Impressum page found for ${job.normalizedUrl}`, success: false },
					pairedItem: { item: job.itemIndex },
				});
				continue;
			}

			const text = htmlToText(job.impressumHtml);
			const data = extractImpressumData(
				job.impressumHtml,
				text,
				job.impressumUrl,
				job.inputUrl,
			);
			successfulJobs.push({ job, data, text });
		}

		// ── Phase 6: Plausibility check + OpenAI enrichment ─────────
		if (openAiKey && successfulJobs.length > 0) {
			await enrichWithOpenAi(this, successfulJobs, openAiKey, openAiModel);
		}

		// ── Push final results ──────────────────────────────────────
		for (const { job, data } of successfulJobs) {
			returnData.push({
				json: { ...data, success: true },
				pairedItem: { item: job.itemIndex },
			});
		}

		return [returnData];
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Batch Page Fetching
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetches pages via direct HTTP (parallel, concurrency-limited).
 * Falls back to batched Apify Cheerio Scraper for failures when token is available.
 */
async function fetchMany(
	ctx: IExecuteFunctions,
	urls: string[],
	timeout: number,
	apifyToken?: string,
): Promise<Map<string, FetchResult>> {
	const results = new Map<string, FetchResult>();
	if (urls.length === 0) return results;

	// Direct HTTP — parallel with concurrency limit
	const failedUrls: string[] = [];

	for (let i = 0; i < urls.length; i += HTTP_CONCURRENCY) {
		const batch = urls.slice(i, i + HTTP_CONCURRENCY);
		const settled = await Promise.allSettled(
			batch.map((url) => fetchDirectSafe(ctx, url, timeout)),
		);

		for (let j = 0; j < batch.length; j++) {
			const r = settled[j];
			if (r.status === 'fulfilled' && r.value) {
				results.set(batch[j], r.value);
			} else {
				failedUrls.push(batch[j]);
			}
		}
	}

	// Apify fallback — batched into parallel actor runs
	if (failedUrls.length > 0 && apifyToken) {
		const apifyResults = await fetchManyApify(ctx, failedUrls, apifyToken);
		for (const [url, result] of apifyResults) {
			results.set(url, result);
		}
	}

	return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Direct HTTP
// ═══════════════════════════════════════════════════════════════════════════════

async function fetchDirectSafe(
	ctx: IExecuteFunctions,
	url: string,
	timeout: number,
): Promise<FetchResult | null> {
	try {
		const response = await ctx.helpers.httpRequest({
			method: 'GET',
			url,
			headers: {
				'User-Agent': USER_AGENT,
				Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
				'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
			},
			returnFullResponse: true,
			timeout,
			ignoreHttpStatusErrors: true,
		});

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const fullResp = response as any;
		if (fullResp.statusCode >= 200 && fullResp.statusCode < 400) {
			const html =
				typeof fullResp.body === 'string' ? fullResp.body : JSON.stringify(fullResp.body);
			return { html, finalUrl: url };
		}
		return null;
	} catch {
		return null;
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Apify Batch Fetching
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetches pages via Apify Cheerio Scraper.
 * - Queries Apify for memory limits, uses at most 3/4 of available memory
 * - Splits URLs into chunks, starts parallel actor runs (512 MB each)
 * - Polls for completion, collects results
 */
async function fetchManyApify(
	ctx: IExecuteFunctions,
	urls: string[],
	apifyToken: string,
): Promise<Map<string, FetchResult>> {
	const results = new Map<string, FetchResult>();
	if (urls.length === 0) return results;

	// ── Determine memory budget ─────────────────────────────────
	let totalMemoryMb = 4096; // Fallback: 4 GB
	try {
		const limitsResp = await ctx.helpers.httpRequest({
			method: 'GET',
			url: 'https://api.apify.com/v2/users/me/limits',
			qs: { token: apifyToken },
		});
		const maxGb = limitsResp?.data?.maxActorMemoryGbytes;
		if (maxGb && maxGb > 0) {
			totalMemoryMb = maxGb * 1024;
		}
	} catch {
		// Use default
	}

	const maxUsableMb = Math.floor(totalMemoryMb * 0.75);
	const memPerRunMb = 512;
	const maxRuns = Math.max(1, Math.floor(maxUsableMb / memPerRunMb));
	const numRuns = Math.min(maxRuns, urls.length);

	// ── Split URLs into chunks ──────────────────────────────────
	const chunkSize = Math.ceil(urls.length / numRuns);
	const chunks: string[][] = [];
	for (let i = 0; i < urls.length; i += chunkSize) {
		chunks.push(urls.slice(i, i + chunkSize));
	}

	const pageFunction = `async function pageFunction(context) {
		return {
			url: context.request.url,
			loadedUrl: context.request.loadedUrl || context.request.url,
			html: context.body,
		};
	}`;

	// ── Start all actor runs in parallel ────────────────────────
	const startResponses = await Promise.allSettled(
		chunks.map((chunk) =>
			ctx.helpers.httpRequest({
				method: 'POST',
				url: 'https://api.apify.com/v2/acts/apify~cheerio-scraper/runs',
				qs: { token: apifyToken, memory: memPerRunMb },
				headers: { 'Content-Type': 'application/json' },
				body: {
					startUrls: chunk.map((u) => ({ url: u })),
					maxRequestsPerCrawl: chunk.length,
					pageFunction,
				},
			}),
		),
	);

	const runIds: string[] = [];
	for (const resp of startResponses) {
		if (resp.status === 'fulfilled' && resp.value?.data?.id) {
			runIds.push(resp.value.data.id);
		}
	}

	if (runIds.length === 0) return results;

	// ── Poll for completion ─────────────────────────────────────
	const completed = new Set<string>();
	const maxWaitMs = 300_000; // 5 minutes
	const pollMs = 3_000;
	const t0 = Date.now();

	while (completed.size < runIds.length && Date.now() - t0 < maxWaitMs) {
		const pending = runIds.filter((id) => !completed.has(id));

		const statusResponses = await Promise.allSettled(
			pending.map((runId) =>
				ctx.helpers.httpRequest({
					method: 'GET',
					url: `https://api.apify.com/v2/actor-runs/${runId}`,
					qs: { token: apifyToken },
				}),
			),
		);

		for (let i = 0; i < pending.length; i++) {
			const resp = statusResponses[i];
			if (resp.status === 'fulfilled') {
				const status = resp.value?.data?.status;
				if (['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT'].includes(status)) {
					completed.add(pending[i]);
				}
			}
		}

		if (completed.size < runIds.length) {
			await new Promise((resolve) => setTimeout(resolve, pollMs));
		}
	}

	// ── Collect results ─────────────────────────────────────────
	const datasetResponses = await Promise.allSettled(
		runIds.map((runId) =>
			ctx.helpers.httpRequest({
				method: 'GET',
				url: `https://api.apify.com/v2/actor-runs/${runId}/dataset/items`,
				qs: { token: apifyToken },
			}),
		),
	);

	for (const resp of datasetResponses) {
		if (resp.status !== 'fulfilled' || !Array.isArray(resp.value)) continue;
		for (const item of resp.value) {
			if (!item.url || !item.html) continue;
			results.set(item.url, { html: item.html, finalUrl: item.loadedUrl || item.url });
			if (item.loadedUrl && item.loadedUrl !== item.url) {
				results.set(item.loadedUrl, { html: item.html, finalUrl: item.loadedUrl });
			}
		}
	}

	return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Plausibility Check + OpenAI Enrichment
// ═══════════════════════════════════════════════════════════════════════════════

const EXTRACTABLE_FIELDS: Record<string, string> = {
	companyName: 'Company or practice name (Firmenname / Praxisname)',
	salutation: 'Salutation: "Herr" or "Frau" only',
	title: 'Academic/professional title (e.g. Dr., Prof., Dr. med., Dr. med. dent.)',
	firstName: 'First name (Vorname)',
	lastName: 'Last name (Nachname)',
	email: 'Email address',
	phone: 'Phone number (Telefon)',
	fax: 'Fax number (Telefax)',
	mobile: 'Mobile number (Mobil / Handy)',
	vatId: 'VAT ID / USt-IdNr (format: DE followed by 9 digits)',
	taxNumber: 'Tax number / Steuernummer',
	street: 'Street address with house number (Straße + Hausnummer)',
	postalCode: 'German postal code, exactly 5 digits (Postleitzahl)',
	city: 'City name (Stadt / Ort)',
	registrationCourt: 'Registration court (Registergericht / Amtsgericht)',
	registrationNumber: 'Registration number (e.g. HRB 12345)',
	chamber: 'Professional chamber (Kammer, e.g. Zahnärztekammer)',
	supervisoryAuthority: 'Supervisory authority (Aufsichtsbehörde, e.g. KZV)',
	professionalTitle: 'Professional title / Berufsbezeichnung (e.g. Zahnarzt)',
	website: 'Website URL',
	managingDirector: 'Managing director (Geschäftsführer/in)',
};

const OPENAI_CONCURRENCY = 5;

/**
 * Checks if regex-extracted data looks plausible.
 * Returns false (= implausible, needs full OpenAI re-parse) when:
 * - Core identity fields are almost entirely missing (no name AND no company)
 * - An email looks like a nav fragment or CSS class rather than a real address
 * - A company name is suspiciously short or looks like a nav item
 * - Address is incomplete (have postalCode but no city, or vice-versa)
 * - Phone number is too short to be real
 */
function isPlausible(data: ImpressumResult): boolean {
	// Must have at least a company name OR a person name
	const hasIdentity =
		(data.companyName && data.companyName.length >= 5) ||
		(data.firstName && data.lastName);
	if (!hasIdentity) return false;

	// Must have at least one contact method
	const hasContact = data.email || data.phone || data.mobile;
	if (!hasContact) return false;

	// Company name sanity: reject nav/menu fragments
	if (data.companyName) {
		const suspicious = /^(Home|Menü|Menu|Startseite|Navigation|Kontakt|Cookie|Datenschutz|Skip|Zum Inhalt)/i;
		if (suspicious.test(data.companyName)) return false;
		if (data.companyName.length < 3) return false;
	}

	// Email sanity
	if (data.email && !/^[\w.+-]+@[\w.-]+\.\w{2,}$/.test(data.email)) return false;

	// Phone sanity: at least 6 digits
	if (data.phone) {
		const digits = data.phone.replace(/\D/g, '');
		if (digits.length < 6) return false;
	}

	// Address consistency: if we have one part, we should have the other
	if ((data.postalCode && !data.city) || (data.city && !data.postalCode)) return false;

	return true;
}

/**
 * 1. Checks plausibility of regex results — implausible items get a full OpenAI re-parse
 * 2. For plausible items with null fields, enriches only the missing fields via OpenAI
 */
async function enrichWithOpenAi(
	ctx: IExecuteFunctions,
	results: Array<{ data: ImpressumResult; text: string }>,
	openAiKey: string,
	model: string,
): Promise<void> {
	const fullReparse: number[] = [];
	const partialEnrich: Array<{ idx: number; missingFields: string[] }> = [];

	for (let i = 0; i < results.length; i++) {
		const data = results[i].data;

		if (!isPlausible(data)) {
			// Implausible → send everything to OpenAI for a full re-parse
			fullReparse.push(i);
		} else {
			// Plausible → only enrich null fields
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const d = data as any;
			const missing = Object.keys(EXTRACTABLE_FIELDS).filter(
				(f) => d[f] === null || d[f] === undefined,
			);
			if (missing.length > 0) {
				partialEnrich.push({ idx: i, missingFields: missing });
			}
		}
	}

	// ── Full re-parse for implausible results ───────────────────
	if (fullReparse.length > 0) {
		const allFields = Object.keys(EXTRACTABLE_FIELDS);

		for (let i = 0; i < fullReparse.length; i += OPENAI_CONCURRENCY) {
			const batch = fullReparse.slice(i, i + OPENAI_CONCURRENCY);
			const responses = await Promise.allSettled(
				batch.map((idx) =>
					callOpenAi(ctx, results[idx].text, allFields, openAiKey, model),
				),
			);

			for (let j = 0; j < batch.length; j++) {
				const resp = responses[j];
				if (resp.status !== 'fulfilled' || !resp.value) continue;

				const idx = batch[j];
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
				const data = results[idx].data as any;
				const extracted = resp.value;

				// Override ALL fields from OpenAI (full re-parse)
				for (const field of allFields) {
					if (extracted[field] != null && extracted[field] !== '') {
						data[field] = String(extracted[field]);
					}
				}
			}
		}
	}

	// ── Partial enrichment for plausible results with gaps ──────
	if (partialEnrich.length > 0) {
		for (let i = 0; i < partialEnrich.length; i += OPENAI_CONCURRENCY) {
			const batch = partialEnrich.slice(i, i + OPENAI_CONCURRENCY);
			const responses = await Promise.allSettled(
				batch.map(({ idx, missingFields }) =>
					callOpenAi(ctx, results[idx].text, missingFields, openAiKey, model),
				),
			);

			for (let j = 0; j < batch.length; j++) {
				const resp = responses[j];
				if (resp.status !== 'fulfilled' || !resp.value) continue;

				const { idx, missingFields } = batch[j];
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
				const data = results[idx].data as any;
				const extracted = resp.value;

				// Only fill null fields — never override regex results
				for (const field of missingFields) {
					if (extracted[field] != null && extracted[field] !== '' && data[field] === null) {
						data[field] = String(extracted[field]);
					}
				}
			}
		}
	}
}

async function callOpenAi(
	ctx: IExecuteFunctions,
	impressumText: string,
	fields: string[],
	apiKey: string,
	model: string,
): Promise<Record<string, string> | null> {
	const fieldList = fields.map((f) => `- ${f}: ${EXTRACTABLE_FIELDS[f]}`).join('\n');
	const truncated = impressumText.length > 4000 ? impressumText.substring(0, 4000) : impressumText;

	try {
		const response = await ctx.helpers.httpRequest({
			method: 'POST',
			url: 'https://api.openai.com/v1/chat/completions',
			headers: {
				Authorization: `Bearer ${apiKey}`,
				'Content-Type': 'application/json',
			},
			body: {
				model,
				temperature: 0,
				response_format: { type: 'json_object' },
				messages: [
					{
						role: 'system',
						content:
							'You extract structured data from German Impressum (legal notice) texts. Return a flat JSON object with ONLY the fields you can confidently identify in the text. Do NOT guess, invent, or hallucinate values. If a field is not clearly present in the text, omit it from the response.',
					},
					{
						role: 'user',
						content: `Extract these fields:\n${fieldList}\n\n---\n${truncated}\n---`,
					},
				],
			},
			timeout: 30000,
		});

		const content = response?.choices?.[0]?.message?.content;
		if (!content) return null;
		return JSON.parse(content);
	} catch {
		return null;
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Impressum Link Finding
// ═══════════════════════════════════════════════════════════════════════════════

function findImpressumUrl(html: string, baseUrl: string): string | null {
	const candidates: Array<{ url: string; score: number }> = [];
	const linkRegex = /<a\s[^>]*href\s*=\s*["']([^"'#]+)["'][^>]*>([\s\S]*?)<\/a>/gi;
	let match;

	while ((match = linkRegex.exec(html)) !== null) {
		const href = match[1].trim();
		const linkText = match[2].replace(/<[^>]+>/g, '').trim().toLowerCase();
		let score = 0;
		const hrefLower = href.toLowerCase();

		if (hrefLower.includes('/impressum')) score += 10;
		if (hrefLower.includes('impressum.html') || hrefLower.includes('impressum.php')) score += 10;
		if (hrefLower.includes('/imprint')) score += 8;
		if (hrefLower.includes('legal-notice') || hrefLower.includes('legal_notice')) score += 6;
		if (linkText === 'impressum') score += 15;
		if (linkText.includes('impressum')) score += 10;
		if (linkText === 'imprint') score += 12;
		if (linkText.includes('imprint')) score += 8;

		if (score > 0) {
			try {
				const resolvedUrl = new URL(href, baseUrl).href;
				candidates.push({ url: resolvedUrl, score });
			} catch {
				/* skip invalid */
			}
		}
	}

	candidates.sort((a, b) => b.score - a.score);
	return candidates.length > 0 ? candidates[0].url : null;
}

function looksLikeImpressum(text: string): boolean {
	const lower = text.toLowerCase();
	const indicators = [
		'angaben gemäß',
		'§ 5 tmg',
		'§ 5 telemediengesetz',
		'pflichtangaben',
		'verantwortlich',
		'telemediengesetz',
	];
	let score = 0;
	for (const ind of indicators) {
		if (lower.includes(ind)) score++;
	}
	if (lower.includes('impressum') && (lower.includes('telefon') || lower.includes('tel.')))
		score++;
	return score >= 1;
}

// ═══════════════════════════════════════════════════════════════════════════════
// HTML to Text
// ═══════════════════════════════════════════════════════════════════════════════

function htmlToText(html: string): string {
	let text = html;
	text = text.replace(/<script[\s\S]*?<\/script>/gi, '');
	text = text.replace(/<style[\s\S]*?<\/style>/gi, '');
	text = text.replace(/<noscript[\s\S]*?<\/noscript>/gi, '');
	text = text.replace(
		/<\/(?:p|div|h[1-6]|li|tr|section|article|header|footer|main|aside|nav)>/gi,
		'\n',
	);
	text = text.replace(/<br\s*\/?>/gi, '\n');
	text = text.replace(/<\/(?:td|th)>/gi, ' ');
	text = text.replace(/<[^>]+>/g, ' ');
	const entities: Record<string, string> = {
		'&amp;': '&',
		'&lt;': '<',
		'&gt;': '>',
		'&quot;': '"',
		'&apos;': "'",
		'&nbsp;': ' ',
		'&ouml;': 'ö',
		'&auml;': 'ä',
		'&uuml;': 'ü',
		'&Ouml;': 'Ö',
		'&Auml;': 'Ä',
		'&Uuml;': 'Ü',
		'&szlig;': 'ß',
	};
	for (const [ent, char] of Object.entries(entities)) {
		text = text.split(ent).join(char);
	}
	text = text.replace(/&#(\d+);/g, (_, code) => String.fromCharCode(parseInt(code)));
	text = text.replace(/&#x([0-9a-fA-F]+);/g, (_, code) =>
		String.fromCharCode(parseInt(code, 16)),
	);
	text = text.replace(/[^\S\n]+/g, ' ');
	text = text
		.split('\n')
		.map((l) => l.trim())
		.join('\n');
	text = text.replace(/\n{3,}/g, '\n\n');
	return text.trim();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Section-Aware Impressum Parsing
// ═══════════════════════════════════════════════════════════════════════════════

interface ImpressumResult {
	sourceUrl: string;
	impressumUrl: string;
	companyName: string | null;
	salutation: string | null;
	title: string | null;
	firstName: string | null;
	lastName: string | null;
	email: string | null;
	phone: string | null;
	fax: string | null;
	mobile: string | null;
	vatId: string | null;
	taxNumber: string | null;
	street: string | null;
	postalCode: string | null;
	city: string | null;
	country: string | null;
	registrationCourt: string | null;
	registrationNumber: string | null;
	chamber: string | null;
	supervisoryAuthority: string | null;
	professionalTitle: string | null;
	website: string | null;
	managingDirector: string | null;
}

function extractImpressumData(
	html: string,
	fullText: string,
	impressumUrl: string,
	sourceUrl: string,
): ImpressumResult {
	const section = isolateImpressumSection(fullText);
	const { business, regulatory } = splitBusinessAndRegulatory(section);

	const person = extractPersonName(business);
	const address = extractAddress(business);
	const registration = extractRegistration(section);

	return {
		sourceUrl,
		impressumUrl,
		companyName: extractCompanyName(business),
		salutation: person.salutation,
		title: person.title,
		firstName: person.firstName,
		lastName: person.lastName,
		email: extractEmail(html, business),
		phone: extractPhone(business),
		fax: extractFax(business),
		mobile: extractMobile(business),
		vatId: extractVatId(section),
		taxNumber: extractTaxNumber(section),
		street: address.street,
		postalCode: address.postalCode,
		city: address.city,
		country: 'Deutschland',
		registrationCourt: registration.court,
		registrationNumber: registration.number,
		chamber: extractChamber(regulatory),
		supervisoryAuthority: extractSupervisoryAuthority(regulatory),
		professionalTitle: extractProfessionalTitle(section),
		website: extractWebsite(business),
		managingDirector: extractManagingDirector(business),
	};
}

function isolateImpressumSection(text: string): string {
	const startPatterns = [
		/Angaben\s+gemäß\s+§\s*5\s*(?:TMG|Telemediengesetz)/i,
		/Pflichtangaben\s+nach\s+§\s*5/i,
		/Impressum\s*[-–—]\s*Pflichtangaben/i,
		/IMPRESSUM\s*\n/,
		/\nImpressum\s*\n/i,
	];
	let startIdx = -1;
	for (const p of startPatterns) {
		const m = text.match(p);
		if (m && m.index !== undefined && (startIdx === -1 || m.index < startIdx)) {
			startIdx = m.index;
		}
	}
	if (startIdx === -1) {
		const m = text.match(/Impressum/i);
		startIdx = m?.index ?? 0;
	}

	const endPatterns = [
		/Haftung\s+für\s+Inhalte/i,
		/Haftungsausschluss/i,
		/Haftung\s+für\s+Links/i,
		/Streitschlichtung/i,
		/Online[\s-]*Streitbeilegung/i,
		/Disclaimer/i,
		/Urheberrecht/i,
		/Copyright\s+©/i,
		/EU[\s-]*Streitschlichtung/i,
		/Verbraucherstreitbeilegung/i,
	];
	let endIdx = text.length;
	for (const p of endPatterns) {
		const m = text.substring(startIdx).match(p);
		if (m && m.index !== undefined) {
			const absIdx = startIdx + m.index;
			if (absIdx > startIdx + 50 && absIdx < endIdx) {
				endIdx = absIdx;
			}
		}
	}

	return text.substring(startIdx, endIdx);
}

function splitBusinessAndRegulatory(section: string): {
	business: string;
	regulatory: string;
} {
	const regulatoryMarkers = [
		/Zuständige\s+(?:Ärzte)?[Kk]ammer/i,
		/Zuständige\s+Aufsichtsbehörde/i,
		/(?:Landes)?[Zz]ahnärztekammer/,
		/Kassenzahnärztliche\s+Vereinigung/,
		/Ärztekammer/,
		/Zuständige\s+Behörde/i,
		/Aufsichtsbehörde/i,
		/Berufsrechtliche\s+Regelungen/i,
	];

	let splitIdx = section.length;
	for (const p of regulatoryMarkers) {
		const m = section.match(p);
		if (m && m.index !== undefined && m.index > 50 && m.index < splitIdx) {
			splitIdx = m.index;
		}
	}

	return {
		business: section.substring(0, splitIdx),
		regulatory: section.substring(splitIdx),
	};
}

// ─── Field Extractors ────────────────────────────────────────────────────────

function extractCompanyName(businessText: string): string | null {
	const lines = businessText
		.split('\n')
		.map((l) => l.trim())
		.filter((l) => l.length > 0);

	const skipPatterns = [
		/^(?:Impressum|Angaben\s+gemäß|Pflichtangaben|IMPRESSUM|Kontakt|Datenschutz|Home|Menü|Navigation)/i,
		/^(?:Tel|Fax|E-?Mail|Telefon|Telefax|Mobil|www\.|http)/i,
		/^\d{5}\s/,
		/^(?:Vertreten|Inhaber|Geschäftsführer|Verantwortlich|Registergericht)/i,
		/^§\s*\d/,
		/^(?:Auftraggeber|Erreichbarkeit)/i,
		/^(?:Startseite|Skip|Zum\s+Inhalt|Navigation\s+überspringen)/i,
	];

	let startLine = 0;
	for (let i = 0; i < lines.length; i++) {
		if (/^(?:Impressum|Angaben\s+gemäß|Pflichtangaben|IMPRESSUM)/i.test(lines[i])) {
			startLine = i + 1;
			break;
		}
	}

	for (let i = startLine; i < Math.min(startLine + 8, lines.length); i++) {
		const line = lines[i];
		if (line.length < 3 || line.length > 150) continue;

		let skip = false;
		for (const p of skipPatterns) {
			if (p.test(line)) {
				skip = true;
				break;
			}
		}
		if (skip) continue;

		if (/[a-zäöüß]/i.test(line) && line.length >= 5) {
			return line;
		}
	}

	return null;
}

interface PersonInfo {
	salutation: string | null;
	title: string | null;
	firstName: string | null;
	lastName: string | null;
}

function extractPersonName(businessText: string): PersonInfo {
	const result: PersonInfo = { salutation: null, title: null, firstName: null, lastName: null };

	const nameContextPatterns = [
		/(?:Inhaber(?:in)?|Praxisinhaber(?:in)?)\s*[.:]\s*\n?\s*(.+)/i,
		/(?:Vertreten\s+durch|Vertretungsberechtigt(?:er)?)\s*[.:]\s*\n?\s*(.+)/i,
		/(?:Geschäftsführer(?:in)?|Geschäftsleitung)\s*[.:]\s*\n?\s*(.+)/i,
		/(?:Verantwortlich\s+(?:für\s+den\s+Inhalt|i\.?\s*S\.?\s*d\.?\s*§|im\s+Sinne|gemäß)[^:]*)\s*[.:]\s*\n?\s*(.+)/i,
		/(?:Inhaltlich\s+Verantwortlich(?:er)?)\s*[.:]\s*\n?\s*(.+)/i,
		/(?:Verantwortlich\s+(?:nach|gem))[^:]*\s*[.:]\s*\n?\s*(.+)/i,
		/(?:Leitung|Praxisleitung)\s*[.:]\s*\n?\s*(.+)/i,
		/(?:Betreiber(?:in)?)\s*[.:]\s*\n?\s*(.+)/i,
		/(?:Zahnärztlicher\s+Leiter(?:in)?)\s*[.:]\s*\n?\s*(.+)/i,
	];

	let nameString: string | null = null;

	for (const pattern of nameContextPatterns) {
		const match = businessText.match(pattern);
		if (match) {
			nameString = match[1].trim();
			if (nameString.length > 80 || /^§|^(?:Die|Der|Das|Ein|Eine)\s/i.test(nameString)) {
				nameString = null;
				continue;
			}
			break;
		}
	}

	if (!nameString) {
		const lines = businessText
			.split('\n')
			.map((l) => l.trim())
			.filter((l) => l.length > 0);
		for (let i = 0; i < lines.length; i++) {
			if (/^\d{5}\s+[A-ZÄÖÜ]/.test(lines[i])) {
				for (let j = Math.max(0, i - 3); j < i; j++) {
					if (looksLikePersonName(lines[j])) {
						nameString = lines[j];
						break;
					}
				}
				break;
			}
		}
	}

	if (!nameString) return result;

	nameString = nameString.split(/\s*(?:&|(?:\s+und\s+))\s*/)[0].trim();
	nameString = nameString.split(/\s*,\s*/)[0].trim();
	nameString = nameString.replace(/\s*\(.*\)/, '');
	nameString = nameString.replace(/\s*[-–].*$/, '');

	const salutationMatch = nameString.match(/^(Herr(?:n)?|Frau)\s+/i);
	if (salutationMatch) {
		result.salutation = salutationMatch[1].replace(/^Herrn$/i, 'Herr');
		nameString = nameString.substring(salutationMatch[0].length).trim();
	}

	const titles: string[] = [];
	let keepExtracting = true;
	while (keepExtracting) {
		keepExtracting = false;
		const titlePatterns = [
			/^Prof\.?\s*/i,
			/^Dr\.?\s*(?:med\.?\s*(?:dent\.?\s*)?)?/i,
			/^Dipl\.?[-\s]\w+\.?\s*/i,
			/^M\.?\s*Sc\.?\s*/i,
			/^B\.?\s*Sc\.?\s*/i,
			/^Dipl\.\s*-?\s*Stom\.?\s*/i,
		];
		for (const pattern of titlePatterns) {
			const m: RegExpMatchArray | null = nameString.match(pattern);
			if (m) {
				titles.push(m[0].trim());
				nameString = nameString.substring(m[0].length).trim();
				keepExtracting = true;
				break;
			}
		}
	}

	if (titles.length > 0) {
		result.title = titles.join(' ');
	}

	const nameParts = nameString.split(/\s+/).filter((p) => p.length > 0);
	const validParts = nameParts.filter(
		(p) =>
			/^[A-ZÄÖÜ][a-zäöüß]+$/.test(p) ||
			/^[A-ZÄÖÜ]\.$/.test(p) ||
			/^[A-ZÄÖÜ][a-zäöüß]+-[A-ZÄÖÜ][a-zäöüß]+$/.test(p),
	);

	if (validParts.length >= 2) {
		result.firstName = validParts[0];
		result.lastName = validParts.slice(1).join(' ');
	} else if (nameParts.length >= 2 && nameParts.every((p) => /^[A-ZÄÖÜ]/.test(p))) {
		result.firstName = nameParts[0];
		result.lastName = nameParts.slice(1).join(' ');
	} else if (nameParts.length === 1 && /^[A-ZÄÖÜ]/.test(nameParts[0])) {
		result.lastName = nameParts[0];
	}

	return result;
}

function looksLikePersonName(line: string): boolean {
	if (line.length < 3 || line.length > 80) return false;
	if (
		/^(Tel|Fax|E-?Mail|Telefon|Telefax|www\.|http|Impressum|Angaben|Kontakt|Vertreten)/i.test(
			line,
		)
	)
		return false;
	if (/^\d{5}/.test(line)) return false;
	if (/@/.test(line)) return false;
	if (/(?:Dr\.|Prof\.|Dipl\.)/i.test(line)) return true;
	if (/(?:Herr|Frau)\s+/i.test(line)) return true;
	const words = line.split(/\s+/);
	const capitalWords = words.filter(
		(w) =>
			/^[A-ZÄÖÜ][a-zäöüß]+$/.test(w) || /^[A-ZÄÖÜ][a-zäöüß]+-[A-ZÄÖÜ][a-zäöüß]+$/.test(w),
	);
	if (capitalWords.length >= 2 && words.length <= 5) return true;
	return false;
}

function extractEmail(html: string, businessText: string): string | null {
	const htmlLower = html.toLowerCase();
	let impressumStart = htmlLower.indexOf('impressum');
	if (impressumStart === -1) impressumStart = 0;
	const impressumHtml = html.substring(impressumStart);

	const mailtoMatch = impressumHtml.match(/mailto:([^\s"'<>?]+)/i);
	if (mailtoMatch) {
		const email = mailtoMatch[1].replace(/&#64;/g, '@').replace(/&#46;/g, '.');
		if (!isChamberEmail(email)) return email;
	}

	const obfuscatedPatterns = [
		/[\w.-]+\s*\(a\)\s*[\w.-]+\.\w{2,}/i,
		/[\w.-]+\s*\[at\]\s*[\w.-]+\.\w{2,}/i,
		/[\w.-]+\s*\(at\)\s*[\w.-]+\.\w{2,}/i,
	];
	for (const p of obfuscatedPatterns) {
		const m = businessText.match(p);
		if (m) {
			const email = m[0]
				.replace(/\s*\(a\)\s*/gi, '@')
				.replace(/\s*\[at\]\s*/gi, '@')
				.replace(/\s*\(at\)\s*/gi, '@');
			if (!isChamberEmail(email)) return email;
		}
	}

	const emailRegex = /[\w.-]+@[\w.-]+\.\w{2,}/g;
	let emailMatch;
	while ((emailMatch = emailRegex.exec(businessText)) !== null) {
		if (!isChamberEmail(emailMatch[0])) return emailMatch[0];
	}

	const fallback = html.match(/mailto:([^\s"'<>?]+)/i);
	if (fallback) return fallback[1].replace(/&#64;/g, '@').replace(/&#46;/g, '.');

	return null;
}

function isChamberEmail(email: string): boolean {
	const chamberDomains = [
		'zaek-sh.de',
		'kzv-sh.de',
		'zaek.de',
		'kzv.de',
		'lzk.de',
		'zaek-nr.de',
		'kzvb.de',
		'lzkh.de',
		'bzaek.de',
	];
	return chamberDomains.some((d) => email.toLowerCase().includes(d));
}

function extractPhone(businessText: string): string | null {
	const patterns = [
		/(?:Tel(?:efon)?|Phone|Fon)\s*[.:]+\s*([+\d][\d\s/\-().]+\d)/i,
		/(?:Tel(?:efon)?|Phone|Fon)\s+([+\d][\d\s/\-().]+\d)/i,
		/T\s*[.:]\s*([+\d][\d\s/\-().]+\d)/,
	];
	for (const p of patterns) {
		const m = businessText.match(p);
		if (m) return m[1].trim();
	}
	return null;
}

function extractFax(businessText: string): string | null {
	const patterns = [
		/(?:Fax|Telefax)\s*[.:]+\s*([+\d][\d\s/\-().]+\d)/i,
		/(?:Fax|Telefax)\s+([+\d][\d\s/\-().]+\d)/i,
	];
	for (const p of patterns) {
		const m = businessText.match(p);
		if (m) return m[1].trim();
	}
	return null;
}

function extractMobile(businessText: string): string | null {
	const m = businessText.match(/(?:Mobil|Handy|Mobile)\s*[.:]\s*([+\d][\d\s/\-().]+\d)/i);
	return m ? m[1].trim() : null;
}

function extractVatId(section: string): string | null {
	const patterns = [
		/USt[\s.-]*Id[\s.-]*Nr\.?\s*[.:]\s*(DE\s?\d{9})/i,
		/Umsatzsteuer[\s-]*Identifikationsnummer[^:]*:\s*(DE\s?\d{9})/i,
		/VAT[\s-]*ID\s*[.:]\s*(\w{2}\s?\d{9})/i,
	];
	for (const p of patterns) {
		const m = section.match(p);
		if (m) return m[1].replace(/\s/g, '');
	}
	return null;
}

function extractTaxNumber(section: string): string | null {
	const patterns = [
		/Steuernummer\s*[.:]\s*([\d\s/]+\d)/i,
		/Steuer[\s-]*Nr\.?\s*[.:]\s*([\d\s/]+\d)/i,
	];
	for (const p of patterns) {
		const m = section.match(p);
		if (m) return m[1].trim();
	}
	return null;
}

function extractAddress(
	businessText: string,
): { street: string | null; postalCode: string | null; city: string | null } {
	const lines = businessText
		.split('\n')
		.map((l) => l.trim())
		.filter((l) => l.length > 0);

	for (let i = 0; i < lines.length; i++) {
		const plzMatch = lines[i].match(
			/^(\d{5})\s+([A-ZÄÖÜ][a-zäöüß]+(?:[\s-][A-Za-zäöüßÄÖÜ]+)*)$/,
		);
		if (!plzMatch) continue;

		const postalCode = plzMatch[1];
		const city = plzMatch[2].trim();
		const plzNum = parseInt(postalCode);
		if (plzNum < 1000 || plzNum > 99999) continue;

		let street: string | null = null;
		if (i > 0) {
			const prevLine = lines[i - 1];
			if (/\d/.test(prevLine) && /[a-zäöüß]/i.test(prevLine) && prevLine.length < 80) {
				street = prevLine;
			}
		}
		return { street, postalCode, city };
	}

	const commaMatch = businessText.match(
		/([A-ZÄÖÜ][a-zäöüß]+(?:[-\s]\w+)*\s+\d+\s*[a-z]?)\s*,\s*(\d{5})\s+([A-ZÄÖÜ][a-zäöüß]+(?:[\s-]\w+)*)/i,
	);
	if (commaMatch) {
		return {
			street: commaMatch[1].trim(),
			postalCode: commaMatch[2],
			city: commaMatch[3].trim(),
		};
	}

	const inlineMatch = businessText.match(
		/([A-ZÄÖÜ][a-zäöüß]+(?:[-\s][A-Za-zäöüßÄÖÜ]+)*(?:str(?:aße|\.)|straße|stra[sß]e|weg|allee|platz|ring|gasse|damm|berg)\s*\d+\s*[a-zA-Z]?)\s*[,\n]\s*(\d{5})\s+([A-ZÄÖÜ][a-zäöüß]+(?:\s+\w+)*)/i,
	);
	if (inlineMatch) {
		return {
			street: inlineMatch[1].trim(),
			postalCode: inlineMatch[2],
			city: inlineMatch[3].trim(),
		};
	}

	return { street: null, postalCode: null, city: null };
}

function extractRegistration(section: string): { court: string | null; number: string | null } {
	const courtMatch = section.match(/(?:Registergericht|Amtsgericht)\s*[.:]\s*([^\n]+)/i);
	let number: string | null = null;

	const numPatterns = [
		/(?:Registernummer|Register[\s-]*Nr\.?)\s*[.:]\s*([^\n,]+)/i,
		/(HRB\s*\d+)/i,
		/(HRA\s*\d+)/i,
	];
	for (const p of numPatterns) {
		const m = section.match(p);
		if (m) {
			number = m[1].trim();
			break;
		}
	}

	return {
		court: courtMatch ? courtMatch[1].trim() : null,
		number,
	};
}

function extractChamber(regulatory: string): string | null {
	const labelMatch = regulatory.match(
		/(?:Zuständige\s+(?:Ärzte)?[Kk]ammer|Kammer)\s*[.:]\s*\n?\s*([^\n]+)/i,
	);
	if (labelMatch) return labelMatch[1].trim();

	const directMatch = regulatory.match(
		/((?:Landes)?[Zz]ahnärztekammer[ \t]+[\w-]+(?:[ \t]+[\w-]+)?)/,
	);
	if (directMatch) return directMatch[1].trim();

	const aeMatch = regulatory.match(/(Ärztekammer[ \t]+[\w-]+(?:[ \t]+[\w-]+)?)/);
	if (aeMatch) return aeMatch[1].trim();

	return null;
}

function extractSupervisoryAuthority(regulatory: string): string | null {
	const labelMatch = regulatory.match(
		/(?:Aufsichtsbehörde|Zuständige\s+(?:Aufsichts)?[Bb]ehörde)\s*[.:]\s*\n?\s*([^\n]+)/i,
	);
	if (labelMatch) return labelMatch[1].trim();

	const directMatch = regulatory.match(
		/(Kassenzahnärztliche[ \t]+Vereinigung[ \t]+[\w-]+(?:[ \t]+[\w-]+)?)/i,
	);
	if (directMatch) return directMatch[1].trim();

	return null;
}

function extractProfessionalTitle(section: string): string | null {
	const m = section.match(/Berufsbezeichnung\s*[.:]\s*\n?\s*([^\n]+)/i);
	return m ? m[1].trim() : null;
}

function extractWebsite(businessText: string): string | null {
	const patterns = [
		/(?:Internet|Web(?:site)?|Homepage|URL)\s*[.:]\s*((?:https?:\/\/)?www\.[\w.-]+\.\w{2,})/i,
		/(?:Internet|Web(?:site)?|Homepage|URL)\s*[.:]\s*(https?:\/\/[\w.-]+\.\w{2,})/i,
	];
	for (const p of patterns) {
		const m = businessText.match(p);
		if (m) return m[1].trim();
	}
	return null;
}

function extractManagingDirector(businessText: string): string | null {
	const patterns = [
		/(?:Geschäftsführer(?:in)?)\s*[.:]\s*\n?\s*([^\n]+)/i,
		/(?:Geschäftsleitung)\s*[.:]\s*\n?\s*([^\n]+)/i,
	];
	for (const p of patterns) {
		const m = businessText.match(p);
		if (m) {
			const name = m[1].trim();
			if (name.length > 2 && name.length < 80) return name;
		}
	}
	return null;
}
