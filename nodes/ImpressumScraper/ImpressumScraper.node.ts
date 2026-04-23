import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';
import { getOpenAiModels } from '../shared/openai-models';

// ═══════════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════════

interface FetchResult {
	html: string;
	finalUrl: string;
}

interface ScrapeJob {
	itemIndex: number;
	companyName: string;
	city: string;
	inputUrl: string;
	normalizedUrl: string;
	homepageHtml?: string;
	homepageFinalUrl?: string;
	impressumUrl?: string | null;
	impressumHtml?: string;
	/** Combined text from fallback pages (contact, about, etc.) when no impressum found */
	fallbackText?: string;
	/** Directory pages from search results that may link to the actual homepage */
	directoryPages?: SearchResult[];
	error?: string;
}

const COMMON_IMPRESSUM_PATHS = [
	'/impressum', '/impressum/', '/impressum.html',
	'/impressum.php', '/imprint', '/imprint/',
];

const FALLBACK_PATHS = [
	'/contact', '/kontakt', '/about', '/about-us', '/ueber-uns',
	'/legal', '/legal-notice', '/disclaimer',
];

const EXCLUDED_DOMAINS = [
	// Social media
	'facebook.', 'instagram.', 'linkedin.', 'twitter.com', 'x.com',
	'youtube.com', 'tiktok.com', 'pinterest.', 'reddit.com', 'tumblr.com',
	// Job boards & career
	'indeed.', 'stepstone.', 'xing.com', 'glassdoor.', 'kununu.com',
	'arbeitsagentur.', 'monster.de', 'stellenanzeigen.de', 'careerjet.',
	'joblift.de', 'jobted.de', 'zfajobs.de', 'www.adecco.com',
	'greatplacetowork.', 'jobbörse.de',
	// Medical/dental directories & portals
	'jameda.de', 'arzt-auskunft.de', 'sanego.de', 'doctolib.',
	'doccheck.com', 'aerzteverzeichnis.de', 'praktischarzt.de',
	'zahnarzt-preisvergleich.com', 'zahnarztgo.com',
	'dental-online-college.com', 'zwp-online.info',
	// Business directories & yellow pages
	'gelbeseiten.de', 'dasoertliche', 'branchenbuch', 'golocal.de',
	'firmenabc.at', 'www.firmenabc.at', 'firmeneintrag.creditreform.de',
	'meinestadt.de', 'cylex.de', '11880.com', 'kennstdueinen.de',
	'marktplatz-mittelstand.de', 'hotfrog.de',
	// Search engines & maps
	'www.google.com', 'google.de/maps', 'maps.google.',
	'bing.com', 'duckduckgo.com',
	// Reference / encyclopedias
	'wikipedia.', 'wikidata.org',
	// Review & rating sites
	'trustpilot.com', 'yelp.', 'bewertung.de',
	// E-commerce & marketplaces
	'ebay.', 'amazon.', 'etsy.com',
	// Hosted platforms (not own domains)
	'wix.com', 'squarespace.com', 'wordpress.com',
	// Swiss/Austrian directories
	'search.ch', 'local.ch',
	// Government registries (impressum is their own, not the company's)
	'handelsregister.de', 'unternehmensregister.de', 'bundesanzeiger.de',
	// Maps & navigation
	'mapquest.com', 'openstreetmap.org',
	// Platform infrastructure (parent companies of directory sites)
	'znanylekarz.pl', 'docplanner.com', 'zendesk.com',
	// Health/dental aggregators & news
	'zahnarztmedizin.de', 'focus-gesundheit.de', 'focus.de/gesundheit',
	'gesundheit.de', 'apotheken-umschau.de',
];

/** Directory domains that often link to the company's actual homepage.
 *  These are a SUBSET of EXCLUDED_DOMAINS — they are not the company's own site,
 *  but their profile pages frequently contain an outgoing "Website" link. */
const HOMEPAGE_LINK_DIRECTORIES = [
	// Medical/dental directories
	'jameda.de', 'doctolib.', 'sanego.de', 'praktischarzt.de',
	'arzt-auskunft.de', 'aerzteverzeichnis.de',
	// Business directories / yellow pages
	'gelbeseiten.de', 'dasoertliche', 'golocal.de',
	'11880.com', 'cylex.de', 'meinestadt.de',
	'kennstdueinen.de', 'hotfrog.de', 'branchenbuch',
	'marktplatz-mittelstand.de', 'firmenabc.at',
];

const HTTP_CONCURRENCY = 30;

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
		subtitle: 'Find company homepage & scrape Impressum',
		description:
			'Searches for a company by name (and optionally city), finds the homepage via Google, then crawls the website to extract structured Impressum/legal data.',
		defaults: {
			name: 'Impressum Scraper',
		},
		inputs: ['main'] as const,
		outputs: ['main'] as const,
		credentials: [
			{
				name: 'searchApi',
				required: true,
			},
			{
				name: 'fachkraftfreundApifyApi',
				required: false,
			},
			{
				name: 'openAiApi',
				required: false,
			},
		],
		properties: [
			{
				displayName: 'Company Name',
				name: 'companyName',
				type: 'string',
				default: '',
				required: true,
				description: 'The company name to search for',
				placeholder: 'Zahnarztpraxis Dr. Müller',
			},
			{
				displayName: 'City',
				name: 'city',
				type: 'string',
				default: '',
				description: 'The city where the company is located (optional – improves search accuracy)',
				placeholder: 'Berlin',
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
						displayName: 'Chunk Size',
						name: 'chunkSize',
						type: 'number',
						default: 100,
						description:
							'Number of items to process through the full pipeline before moving to the next chunk. Lower values use less memory and recover faster from rate limits. Set to 0 to process all items at once (legacy behavior).',
					},
					{
						displayName: 'Country Code',
						name: 'country',
						type: 'string',
						default: 'de',
						description: 'ISO country code for Google search localization (e.g. de, at, ch)',
					},
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
			getOpenAiModels,
		},
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		// ── Configuration ───────────────────────────────────────────
		const searchApiCreds = await this.getCredentials('searchApi');
		const searchApiKey = searchApiCreds.apiKey as string;

		let apifyToken: string | undefined;
		try {
			const creds = await this.getCredentials('fachkraftfreundApifyApi');
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
			country?: string;
			chunkSize?: number;
		};
		const timeout = (options.timeout ?? 15) * 1000;
		const searchTimeout = Math.max(timeout, 30000);
		const tryCommonPaths = options.tryCommonPaths !== false;
		const checkHomepage = options.checkHomepage !== false;
		const country = options.country || 'de';
		const chunkSize = (options.chunkSize ?? 100) > 0 ? (options.chunkSize ?? 100) : items.length;

		// ── Chunked processing: process N items through the full pipeline at a time ──
		for (let chunkStart = 0; chunkStart < items.length; chunkStart += chunkSize) {
		const chunkEnd = Math.min(chunkStart + chunkSize, items.length);

		const jobs: ScrapeJob[] = [];
		for (let i = chunkStart; i < chunkEnd; i++) {
			const companyName = this.getNodeParameter('companyName', i) as string;
			const city = (this.getNodeParameter('city', i, '') as string) || '';
			jobs.push({ itemIndex: i, companyName, city, inputUrl: '', normalizedUrl: '' });
		}

		// ── Phase 0a: Google search for all items ─────────────────
		// Split search into sub-phases to avoid head-of-line blocking:
		// a single item needing Google+Bing+OpenAI (3.5s) no longer blocks
		// items that only need Google (1s).
		const SEARCH_CONCURRENCY = 20;
		const jobQueries = new Map<ScrapeJob, string>();
		const jobGoogleResults = new Map<ScrapeJob, { all: SearchResult[]; filtered: SearchResult[] }>();

		for (let i = 0; i < jobs.length; i += SEARCH_CONCURRENCY) {
			const batch = jobs.slice(i, i + SEARCH_CONCURRENCY);
			const settled = await Promise.allSettled(
				batch.map(async (job) => {
					const normalizedName = job.companyName.toLowerCase().replace(/[^a-zäöüß\s]/g, '');
					const normalizedCity = job.city.toLowerCase().replace(/[^a-zäöüß\s]/g, '');
					const query = normalizedName.includes(normalizedCity)
						? job.companyName
						: `${job.companyName} ${job.city}`;
					jobQueries.set(job, query);

					const allRawResults = await searchWeb(this, query, country, searchApiKey, 'google', searchTimeout);
					const filtered = filterSearchResults(allRawResults);
					jobGoogleResults.set(job, { all: allRawResults, filtered });
				}),
			);
			for (let j = 0; j < batch.length; j++) {
				if (settled[j].status === 'rejected') {
					const reason = (settled[j] as PromiseRejectedResult).reason;
					if (isQuotaError(reason)) {
						throw new NodeOperationError(
							this.getNode(),
							`SearchAPI quota exhausted (HTTP 429). Check your SearchAPI plan or wait before retrying.`,
						);
					}
					batch[j].error = `Search failed: ${reason?.message || 'Unknown error'}`;
				}
			}
		}

		// ── Phase 0a2: Bing fallback for items that need it ───────
		const bingJobs = jobs.filter((job) => {
			if (job.error) return false;
			const gr = jobGoogleResults.get(job);
			if (!gr) return false; // No Google results entry → skip (shouldn't happen)
			return gr.filtered.length === 0 || !hasLikelyOwnWebsite(gr.filtered, job.companyName, job.city);
		});

		if (bingJobs.length > 0) {
			for (let i = 0; i < bingJobs.length; i += SEARCH_CONCURRENCY) {
				const batch = bingJobs.slice(i, i + SEARCH_CONCURRENCY);
				const bingSettled = await Promise.allSettled(
					batch.map(async (job) => {
						const query = jobQueries.get(job) || job.companyName;
						const bingResults = await searchWeb(this, query, country, searchApiKey, 'bing', searchTimeout);
						const gr = jobGoogleResults.get(job)!;
						gr.all = [...gr.all, ...bingResults];
						const bingFiltered = filterSearchResults(bingResults);
						if (bingFiltered.length > 0) {
							const seen = new Set(bingFiltered.map((r) => r.link));
							gr.filtered = [...bingFiltered, ...gr.filtered.filter((r) => !seen.has(r.link))];
						}
					}),
				);
				for (const s of bingSettled) {
					if (s.status === 'rejected' && isQuotaError(s.reason)) {
						throw new NodeOperationError(
							this.getNode(),
							`SearchAPI quota exhausted (HTTP 429). Check your SearchAPI plan or wait before retrying.`,
						);
					}
				}
			}
		}

		// ── Phase 0a3: Collect directory pages + resolve homepages ──
		// First, extract directory pages and identify items needing OpenAI
		const aiPickJobs: Array<{ job: ScrapeJob; query: string; filtered: SearchResult[] }> = [];

		for (const job of jobs) {
			if (job.error) continue;
			const gr = jobGoogleResults.get(job);
			if (!gr) continue;
			const query = jobQueries.get(job) || job.companyName;

			// Collect directory pages
			const dirPages = gr.all.filter((r) => {
				try {
					const hostname = new URL(r.link).hostname;
					return HOMEPAGE_LINK_DIRECTORIES.some((d) => hostname.includes(d));
				} catch { return false; }
			});
			const seenDir = new Set<string>();
			job.directoryPages = dirPages.filter((r) => {
				if (seenDir.has(r.link)) return false;
				seenDir.add(r.link);
				return true;
			});

			if (gr.filtered.length === 0 && job.directoryPages.length === 0) {
				job.error = job.city
					? `No search results found for "${job.companyName}" in "${job.city}"`
					: `No search results found for "${job.companyName}"`;
				continue;
			}

			// Items with >1 result: try heuristic first, fall back to OpenAI
			if (gr.filtered.length > 1) {
				const heuristicPick = pickHomepageByDomain(gr.filtered, job.companyName, job.city);
				if (heuristicPick) {
					const homepageUrl = heuristicPick;
					job.inputUrl = homepageUrl;
					let normalizedUrl = homepageUrl.trim();
					if (!/^https?:\/\//i.test(normalizedUrl)) normalizedUrl = 'https://' + normalizedUrl;
					try { new URL(normalizedUrl); job.normalizedUrl = normalizedUrl; }
					catch { job.error = `Invalid URL resolved from search: ${homepageUrl}`; }
				} else if (openAiKey) {
					aiPickJobs.push({ job, query, filtered: gr.filtered });
				} else {
					// No OpenAI key and heuristic failed — use first result
					const homepageUrl = gr.filtered[0].link;
					job.inputUrl = homepageUrl;
					let normalizedUrl = homepageUrl.trim();
					if (!/^https?:\/\//i.test(normalizedUrl)) normalizedUrl = 'https://' + normalizedUrl;
					try { new URL(normalizedUrl); job.normalizedUrl = normalizedUrl; }
					catch { job.error = `Invalid URL resolved from search: ${homepageUrl}`; }
				}
			} else if (gr.filtered.length > 0) {
				// Only one result — use it directly
				const homepageUrl = gr.filtered[0].link;
				job.inputUrl = homepageUrl;
				let normalizedUrl = homepageUrl.trim();
				if (!/^https?:\/\//i.test(normalizedUrl)) normalizedUrl = 'https://' + normalizedUrl;
				try { new URL(normalizedUrl); job.normalizedUrl = normalizedUrl; }
				catch { job.error = `Invalid URL resolved from search: ${homepageUrl}`; }
			}
		}

		// ── Phase 0a4: OpenAI homepage selection (parallel) ──────
		if (aiPickJobs.length > 0) {
			for (let i = 0; i < aiPickJobs.length; i += OPENAI_CONCURRENCY) {
				const batch = aiPickJobs.slice(i, i + OPENAI_CONCURRENCY);
				await Promise.allSettled(
					batch.map(async ({ job, query, filtered }) => {
						let homepageUrl = await findHomepageWithOpenAi(this, query, filtered, openAiKey!, openAiModel);
						if (!homepageUrl && filtered.length > 0) {
							homepageUrl = filtered[0].link;
						}
						if (homepageUrl) {
							job.inputUrl = homepageUrl;
							let normalizedUrl = homepageUrl.trim();
							if (!/^https?:\/\//i.test(normalizedUrl)) normalizedUrl = 'https://' + normalizedUrl;
							try { new URL(normalizedUrl); job.normalizedUrl = normalizedUrl; }
							catch { job.error = `Invalid URL resolved from search: ${homepageUrl}`; }
						}
					}),
				);
			}
		}

		// Clean up search result maps to free memory
		jobQueries.clear();
		jobGoogleResults.clear();

		// ── Phase 0b: Directory homepage discovery (fallback) ──────
		const jobsNeedingHomepage = jobs.filter(
			(j) => !j.error && !j.normalizedUrl && j.directoryPages && j.directoryPages.length > 0,
		);

		if (jobsNeedingHomepage.length > 0) {
			const allDirUrls: string[] = [];
			for (const job of jobsNeedingHomepage) {
				for (const r of job.directoryPages!.slice(0, 3)) {
					allDirUrls.push(r.link);
				}
			}
			const uniqueDirUrls = [...new Set(allDirUrls)];
			const dirFetchResults = await fetchMany(this, uniqueDirUrls, timeout, apifyToken);

			for (const job of jobsNeedingHomepage) {
				const dirUrls = job.directoryPages!.slice(0, 3).map((r) => r.link);
				for (const dirUrl of dirUrls) {
					const result = dirFetchResults.get(dirUrl);
					if (result && result.html) {
						const discovered = extractHomepageLinkFromDirectory(result.html, dirUrl);
						if (discovered) {
							job.inputUrl = discovered;
							let normalizedUrl = discovered.trim();
							if (!/^https?:\/\//i.test(normalizedUrl)) {
								normalizedUrl = 'https://' + normalizedUrl;
							}
							try {
								new URL(normalizedUrl);
								job.normalizedUrl = normalizedUrl;
								break;
							} catch { /* skip invalid */ }
						}
					}
				}
				if (!job.normalizedUrl) {
					job.error = job.city
						? `No homepage found for "${job.companyName}" in "${job.city}" (checked ${dirUrls.length} directory pages)`
						: `No homepage found for "${job.companyName}" (checked ${dirUrls.length} directory pages)`;
				}
			}
		}

		// ── Phase 0c: Domain guessing fallback (deterministic + AI) ──
		{
			const jobsStillNeedingHomepage = jobs.filter(
				(j) => !j.normalizedUrl && !j.error,
			);
			// Also include jobs that got an error in Phase 0b (directory failure)
			const jobsFromDirFailure = jobs.filter(
				(j) => j.error && j.error.includes('checked') && j.error.includes('directory pages'),
			);
			const allGuessJobs = [...jobsStillNeedingHomepage, ...jobsFromDirFailure];

			if (allGuessJobs.length > 0) {
				// Step 1: Try deterministic guesses (no AI needed)
				const verifyAndAssign = async (job: ScrapeJob, guesses: string[]): Promise<boolean> => {
					if (guesses.length === 0) return false;
					const checks = await Promise.allSettled(
						guesses.slice(0, 8).map(async (domain: string) => {
							const exists = await domainExists(this, domain, timeout);
							return { domain, exists };
						}),
					);
					for (const check of checks) {
						if (check.status === 'fulfilled' && check.value.exists) {
							const url = `https://${check.value.domain}`;
							job.inputUrl = url;
							job.normalizedUrl = url;
							job.error = undefined;
							return true;
						}
					}
					return false;
				};

				for (let i = 0; i < allGuessJobs.length; i += OPENAI_CONCURRENCY) {
					const batch = allGuessJobs.slice(i, i + OPENAI_CONCURRENCY);
					await Promise.allSettled(
						batch.map(async (job) => {
							const localGuesses = guessDomainsLocal(job.companyName, job.city);
							await verifyAndAssign(job, localGuesses);
						}),
					);
				}

				// Step 2: AI fallback for jobs where deterministic guesses failed
				if (openAiKey) {
					const aiGuessJobs = allGuessJobs.filter((j) => !j.normalizedUrl || j.error);
					for (let i = 0; i < aiGuessJobs.length; i += OPENAI_CONCURRENCY) {
						const batch = aiGuessJobs.slice(i, i + OPENAI_CONCURRENCY);
						await Promise.allSettled(
							batch.map(async (job) => {
								const aiGuesses = await guessDomainsAi(this, job.companyName, job.city, openAiKey!, openAiModel);
								await verifyAndAssign(job, aiGuesses);
							}),
						);
					}
				}
			}
		}

		// Mark remaining jobs with no homepage and no error
		for (const job of jobs) {
			if (!job.error && !job.normalizedUrl) {
				job.error = job.city
					? `No homepage found for "${job.companyName}" in "${job.city}"`
					: `No homepage found for "${job.companyName}"`;
			}
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

		// ── Phase 3: Try common paths (wave-based: stop early per item) ──
		if (tryCommonPaths) {
			let jobsNeedingPaths = jobsWithHomepage.filter((j) => !j.impressumUrl);

			for (const path of COMMON_IMPRESSUM_PATHS) {
				if (jobsNeedingPaths.length === 0) break;

				const urlToJobs = new Map<string, ScrapeJob[]>();
				for (const job of jobsNeedingPaths) {
					try {
						const base = new URL(job.homepageFinalUrl || job.normalizedUrl);
						const url = new URL(path, base).href;
						const list = urlToJobs.get(url) || [];
						list.push(job);
						urlToJobs.set(url, list);
					} catch { /* skip invalid */ }
				}

				const waveResults = await fetchMany(this, [...urlToJobs.keys()], timeout, apifyToken);

				for (const [url, jobList] of urlToJobs) {
					const result = waveResults.get(url);
					if (result && result.html.length > 500) {
						const text = htmlToText(result.html);
						if (looksLikeImpressum(text)) {
							for (const job of jobList) {
								job.impressumUrl = url;
								job.impressumHtml = result.html;
							}
						}
					}
				}

				// Only keep items that still need an impressum for the next wave
				jobsNeedingPaths = jobsNeedingPaths.filter((j) => !j.impressumUrl);
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
					// Impressum page is dead — clear it so fallback kicks in
					job.impressumUrl = null;
				}
			}
		}

		// ── Phase 4b: Fallback pages for jobs without impressum ─────
		const jobsWithoutImpressum = jobsWithHomepage.filter(
			(j) => (!j.impressumUrl || !j.impressumHtml) && !j.error,
		);

		if (jobsWithoutImpressum.length > 0) {
			// Find contact/about/legal links from homepage HTML
			const fallbackCandidates: Array<{ url: string; jobIdx: number }> = [];
			for (let ji = 0; ji < jobsWithoutImpressum.length; ji++) {
				const job = jobsWithoutImpressum[ji];
				const baseUrl = job.homepageFinalUrl || job.normalizedUrl;

				// Scan homepage for contact/about/legal links
				const foundUrls = findFallbackUrls(job.homepageHtml!, baseUrl);
				for (const u of foundUrls) {
					fallbackCandidates.push({ url: u, jobIdx: ji });
				}

				// Also try common fallback paths
				const base = new URL(baseUrl);
				for (const path of FALLBACK_PATHS) {
					fallbackCandidates.push({ url: new URL(path, base).href, jobIdx: ji });
				}
			}

			if (fallbackCandidates.length > 0) {
				const uniqueUrls = [...new Set(fallbackCandidates.map((c) => c.url))];
				const fallbackResults = await fetchMany(this, uniqueUrls, timeout, apifyToken);

				// Group fetched texts per job
				const textsPerJob = new Map<number, string[]>();
				for (const { url, jobIdx } of fallbackCandidates) {
					const result = fallbackResults.get(url);
					if (result && result.html.length > 200) {
						if (!textsPerJob.has(jobIdx)) textsPerJob.set(jobIdx, []);
						textsPerJob.get(jobIdx)!.push(htmlToText(result.html));
					}
				}

				for (let ji = 0; ji < jobsWithoutImpressum.length; ji++) {
					const job = jobsWithoutImpressum[ji];
					const parts: string[] = [];
					// Always include homepage text
					if (job.homepageHtml) parts.push(htmlToText(job.homepageHtml));
					// Add fallback page texts
					const extra = textsPerJob.get(ji);
					if (extra) parts.push(...extra);
					if (parts.length > 0) {
						job.fallbackText = parts.join('\n\n---\n\n');
					}
				}
			}
		}

		// ── Phase 5: Parse results ──────────────────────────────────
		const successfulJobs: Array<{ job: ScrapeJob; data: ImpressumResult; text: string }> = [];

		for (const job of jobs) {
			if (job.error) {
				returnData.push({
					json: { inputCompanyName: job.companyName, inputCity: job.city, sourceUrl: job.inputUrl, error: job.error, success: false },
					pairedItem: { item: job.itemIndex },
				});
				continue;
			}

			if (job.impressumUrl && job.impressumHtml) {
				// Standard path: dedicated impressum page found
				const text = htmlToText(job.impressumHtml);
				const data = extractImpressumData(
					job.impressumHtml,
					text,
					job.impressumUrl,
					job.inputUrl,
				);
				successfulJobs.push({ job, data, text });
			} else if (job.fallbackText) {
				// Fallback path: no impressum found, try extracting from homepage + fallback pages
				const html = job.homepageHtml || '';
				const text = job.fallbackText;
				const data = extractImpressumData(html, text, job.normalizedUrl, job.inputUrl);
				successfulJobs.push({ job, data, text });
			} else {
				returnData.push({
					json: { inputCompanyName: job.companyName, inputCity: job.city, sourceUrl: job.inputUrl, error: `No Impressum page found for ${job.normalizedUrl}`, success: false },
					pairedItem: { item: job.itemIndex },
				});
			}
		}

		// Free fallbackText — no longer needed after Phase 5 parsing
		for (const job of jobs) job.fallbackText = undefined;

		// ── Phase 5b: Fill gaps from homepage HTML ──────────────────
		for (const { job, data } of successfulJobs) {
			if (!job.homepageHtml) continue;
			// Skip if impressum was already the homepage
			if (job.impressumUrl === job.homepageFinalUrl || job.impressumUrl === job.normalizedUrl) continue;

			// Check if there are empty fields worth filling
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const d = data as any;
			const emptyFields = Object.keys(EXTRACTABLE_FIELDS).filter((f) =>
				ARRAY_FIELDS.has(f) ? (d[f] as unknown[]).length === 0 : d[f] === null,
			);
			if (emptyFields.length === 0) continue;

			const homepageText = htmlToText(job.homepageHtml);
			const homepageData = extractImpressumData(
				job.homepageHtml,
				homepageText,
				job.normalizedUrl,
				job.inputUrl,
			);

			// Only fill empty fields — never override impressum-extracted values
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const hd = homepageData as any;
			for (const field of emptyFields) {
				if (ARRAY_FIELDS.has(field)) {
					if ((hd[field] as unknown[]).length > 0) d[field] = hd[field];
				} else if (hd[field] != null) {
					d[field] = hd[field];
				}
			}
		}

		// ── Phase 5c: JS fallback for placeholder-obfuscated emails ──
		if (apifyToken) {
			const jsRefetchJobs = successfulJobs.filter(({ job, data }) => {
				if (data.emails.length > 0) return false;
				const html = job.impressumHtml || job.homepageHtml || '';
				return htmlHasPlaceholderEmails(html);
			});

			if (jsRefetchJobs.length > 0) {
				const urlsToRefetch: string[] = [];
				const urlToJobIdx = new Map<string, number[]>();
				for (let i = 0; i < jsRefetchJobs.length; i++) {
					const { job } = jsRefetchJobs[i];
					const url = job.impressumUrl || job.normalizedUrl;
					if (url) {
						urlsToRefetch.push(url);
						const existing = urlToJobIdx.get(url) || [];
						existing.push(i);
						urlToJobIdx.set(url, existing);
					}
				}

				const jsResults = await fetchWithJsApify(this, [...new Set(urlsToRefetch)], apifyToken);

				for (const [url, result] of jsResults) {
					const indices = urlToJobIdx.get(url) || [];
					for (const idx of indices) {
						const { job, data } = jsRefetchJobs[idx];
						let jsDomain: string | undefined;
						try { jsDomain = new URL(job.normalizedUrl).hostname.replace(/^www\./, ''); } catch { /* ignore */ }
						const jsText = htmlToText(result.html);
						const jsEmails = extractEmails(result.html, jsText, jsDomain);
						if (jsEmails.length > 0) {
							data.emails = jsEmails;
							// Update stored HTML so later phases benefit too
							if (job.impressumHtml) job.impressumHtml = result.html;
						}
					}
				}
			}
		}

		// Free HTML strings — no longer needed after Phase 5c
		// For 10k items this reclaims ~2GB; even with chunking it reduces peak memory
		for (const job of jobs) {
			job.homepageHtml = undefined;
			job.impressumHtml = undefined;
		}

		// ── Phase 5d: Aggregator / directory detection ──────────────
		// When ≥ 3 different input companies in the same batch end up on the
		// exact same impressumUrl, the page is an aggregator's own impressum
		// (e.g. ziegler-physiotherapie.de, suchehandwerker.de, physio.de).
		// Reject those scrapes — the extracted data belongs to the directory,
		// not the queried company.
		{
			const impressumCounts = new Map<string, number>();
			for (const { job } of successfulJobs) {
				if (job.impressumUrl) {
					impressumCounts.set(job.impressumUrl, (impressumCounts.get(job.impressumUrl) || 0) + 1);
				}
			}
			const aggregatedUrls = new Set<string>();
			for (const [url, n] of impressumCounts) {
				if (n >= 3) aggregatedUrls.add(url);
			}
			if (aggregatedUrls.size > 0) {
				const kept: Array<{ job: ScrapeJob; data: ImpressumResult; text: string }> = [];
				for (const entry of successfulJobs) {
					const url = entry.job.impressumUrl;
					if (url && aggregatedUrls.has(url)) {
						returnData.push({
							json: {
								inputCompanyName: entry.job.companyName,
								inputCity: entry.job.city,
								sourceUrl: entry.job.inputUrl,
								impressumUrl: url,
								error: `Aggregator impressum hit by ${impressumCounts.get(url)} jobs in this batch — rejected to avoid overwriting real company data with directory data`,
								success: false,
							},
							pairedItem: { item: entry.job.itemIndex },
						});
					} else {
						kept.push(entry);
					}
				}
				successfulJobs.length = 0;
				successfulJobs.push(...kept);
			}
		}

		// ── Phase 6: Plausibility check + OpenAI enrichment ─────────
		if (openAiKey && successfulJobs.length > 0) {
			await enrichWithOpenAi(this, successfulJobs, openAiKey, openAiModel);
		}

		// ── Phase 6b: Focused person-recovery for rows where the scraper
		// produced a valid companyName but no lastName. A dedicated OpenAI
		// call with a prompt tailored to "find the primary responsible
		// person in this Impressum" recovers cases where the generic
		// enrichment missed a name that is actually on the page.
		if (openAiKey && successfulJobs.length > 0) {
			await recoverMissingPersons(this, successfulJobs, openAiKey, openAiModel);
		}

		// Free impressum text — no longer needed after enrichment
		for (const entry of successfulJobs) (entry as { text: string | undefined }).text = undefined;

		// ── Phase 7: Derive salutation from firstName via OpenAI ───
		if (openAiKey && successfulJobs.length > 0) {
			await deriveSalutations(this, successfulJobs, openAiKey, openAiModel);
		}

		// ── Phase 8: Normalize emails ──────────────────────────────
		for (const { data } of successfulJobs) {
			data.emails = data.emails.map(normalizeEmail);
		}

		// ── Phase 8b: Prioritize emails ─────────────────────────
		for (const { data } of successfulJobs) {
			data.emails = prioritizeEmails(data.emails, data.firstName, data.lastName);
		}

		// ── Phase 9: Normalize phone numbers via OpenAI ──────────
		if (openAiKey && successfulJobs.length > 0) {
			await normalizePhoneNumbers(this, successfulJobs, openAiKey, openAiModel);
		}

		// ── Sanitize and push chunk results ─────────────────────────
		for (const { job, data } of successfulJobs) {
			sanitizeResult(data);
			returnData.push({
				json: { inputCompanyName: job.companyName, inputCity: job.city, ...data, success: true },
				pairedItem: { item: job.itemIndex },
			});
		}

		} // ── end chunk loop ─────────────────────────────────────────

		returnData.sort((a, b) => (a.pairedItem as { item: number }).item - (b.pairedItem as { item: number }).item);
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
	const MAX_ATTEMPTS = 2;
	for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
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
			// Non-retriable HTTP error (4xx/5xx) — don't retry
			return null;
		} catch {
			if (attempt < MAX_ATTEMPTS - 1) {
				await new Promise((r) => setTimeout(r, 2000));
				continue;
			}
			return null;
		}
	}
	return null;
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
			timeout: 15000,
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
				timeout: 30000,
			}),
		),
	);

	const runIds: string[] = [];
	for (const resp of startResponses) {
		if (resp.status === 'rejected' && isQuotaError(resp.reason)) {
			throw new NodeOperationError(
				ctx.getNode(),
				`Apify budget depleted. Check your Apify subscription or wait for the budget to reset.`,
			);
		}
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
					timeout: 15000,
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
				timeout: 60000,
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
// Apify Web Scraper (JS-enabled fallback)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetches pages via Apify Web Scraper (Puppeteer-based), which executes JavaScript.
 * Used as a targeted fallback when placeholder/obfuscated emails are detected in
 * static HTML, since the real addresses are often injected by client-side JS.
 */
async function fetchWithJsApify(
	ctx: IExecuteFunctions,
	urls: string[],
	apifyToken: string,
): Promise<Map<string, FetchResult>> {
	const results = new Map<string, FetchResult>();
	if (urls.length === 0) return results;

	const pageFunction = `async function pageFunction(context) {
		return {
			url: context.request.url,
			html: await context.page.content(),
		};
	}`;

	let resp;
	try {
		resp = await ctx.helpers.httpRequest({
			method: 'POST',
			url: 'https://api.apify.com/v2/acts/apify~web-scraper/runs',
			qs: { token: apifyToken, memory: 1024 },
			headers: { 'Content-Type': 'application/json' },
			body: {
				startUrls: urls.map((u) => ({ url: u })),
				maxRequestsPerCrawl: urls.length,
				pageFunction,
			},
			timeout: 30000,
		});
	} catch (err) {
		if (isQuotaError(err)) {
			throw new NodeOperationError(
				ctx.getNode(),
				`Apify budget depleted. Check your Apify subscription or wait for the budget to reset.`,
			);
		}
		return results;
	}

	const runId = resp?.data?.id;
	if (!runId) return results;

	// Poll for completion
	const maxWaitMs = 120_000; // 2 minutes (small batch, single run)
	const pollMs = 3_000;
	const t0 = Date.now();
	let done = false;

	while (!done && Date.now() - t0 < maxWaitMs) {
		try {
			const statusResp = await ctx.helpers.httpRequest({
				method: 'GET',
				url: `https://api.apify.com/v2/actor-runs/${runId}`,
				qs: { token: apifyToken },
				timeout: 15000,
			});
			const status = statusResp?.data?.status;
			if (['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT'].includes(status)) {
				done = true;
			}
		} catch {
			// ignore polling errors
		}
		if (!done) await new Promise((r) => setTimeout(r, pollMs));
	}

	// Collect results
	try {
		const items = await ctx.helpers.httpRequest({
			method: 'GET',
			url: `https://api.apify.com/v2/actor-runs/${runId}/dataset/items`,
			qs: { token: apifyToken },
			timeout: 60000,
		});
		if (Array.isArray(items)) {
			for (const item of items) {
				if (item.url && item.html) {
					results.set(item.url, { html: item.html, finalUrl: item.url });
				}
			}
		}
	} catch {
		// ignore
	}

	return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Quota / rate-limit detection
// ═══════════════════════════════════════════════════════════════════════════════

function isQuotaError(err: unknown): boolean {
	if (!err) return false;
	const msg = (err as Error)?.message || String(err);
	if (/status code 429/i.test(msg)) return true;
	if (/status code 402/i.test(msg)) return true;
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const code = (err as any)?.statusCode ?? (err as any)?.response?.status;
	return code === 429 || code === 402;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Google Search via SearchAPI
// ═══════════════════════════════════════════════════════════════════════════════

interface SearchResult {
	title: string;
	link: string;
}

async function searchWeb(
	ctx: IExecuteFunctions,
	query: string,
	country: string,
	apiKey: string,
	engine: 'google' | 'bing' = 'google',
	timeout: number = 30000,
): Promise<SearchResult[]> {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const qs: Record<string, any> = {
		q: query,
		engine,
		api_key: apiKey,
	};
	if (engine === 'google') qs.gl = country;

	const response = await ctx.helpers.httpRequest({
		method: 'GET',
		url: 'https://www.searchapi.io/api/v1/search',
		qs,
		timeout,
	});

	return (response?.organic_results || [])
		.map((r: { title?: string; link?: string }) => ({
			title: r.title || '',
			link: r.link || '',
		}))
		.filter((r: SearchResult) => r.link);
}

/**
 * Heuristic homepage picker: scores each result's domain against company name keywords.
 * Returns the URL of the best match if one result clearly stands out, or null if ambiguous.
 */
function pickHomepageByDomain(results: SearchResult[], companyName: string, city: string): string | null {
	const UMLAUT_MAP: Record<string, string> = { 'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss' };
	const normalize = (s: string) =>
		s.toLowerCase().replace(/[äöüß]/g, (c) => UMLAUT_MAP[c] || c).replace(/[^a-z0-9\s]/g, '');

	const SKIP = /^(zahnarzt|praxis|zahnarztpraxis|kieferorthopaedie|kiefer|orthopaede|arzt|arztpraxis|klinik|zentrum|institut|gmbh|gbr|ohg|ug|dr|prof|med|dent|dipl|und|am|im|an|der|die|das|den|dem|fuer|von|zu|zur|zum)$/;

	const keywords = [...normalize(companyName).split(/\s+/), ...(city ? normalize(city).split(/\s+/) : [])]
		.filter((w) => w.length >= 3 && !SKIP.test(w));

	if (keywords.length === 0) return null;

	const scored: Array<{ link: string; score: number }> = [];
	for (const r of results) {
		try {
			const hostname = new URL(r.link).hostname.toLowerCase();
			const matchCount = keywords.filter((kw) => hostname.includes(kw)).length;
			scored.push({ link: r.link, score: matchCount });
		} catch { scored.push({ link: r.link, score: 0 }); }
	}

	scored.sort((a, b) => b.score - a.score);

	// Only pick if the top result has matches AND clearly beats the runner-up
	if (scored[0].score >= 1 && scored[0].score > (scored[1]?.score ?? 0)) {
		return scored[0].link;
	}
	return null;
}

/**
 * Heuristic: do any of the filtered results look like they belong to the company's
 * own website? Extracts keywords from the company name/city and checks if any
 * result's domain contains them. If not, we should try another search engine.
 */
function hasLikelyOwnWebsite(results: SearchResult[], companyName: string, city: string): boolean {
	const UMLAUT_MAP: Record<string, string> = { 'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss' };
	const normalize = (s: string) =>
		s.toLowerCase().replace(/[äöüß]/g, (c) => UMLAUT_MAP[c] || c).replace(/[^a-z\s]/g, '');

	// Skip generic words that don't help identify a specific company's domain
	const SKIP = /^(zahnarzt|praxis|zahnarztpraxis|kieferorthopaedie|kiefer|orthopaede|arzt|arztpraxis|klinik|zentrum|institut|gmbh|gbr|ohg|ug|dr|prof|med|dent|dipl|und|am|im|an|der|die|das|den|dem|fuer|von|zu|zur|zum)$/;

	const keywords = [...normalize(companyName).split(/\s+/), ...(city ? normalize(city).split(/\s+/) : [])]
		.filter((w) => w.length >= 3 && !SKIP.test(w));

	if (keywords.length === 0) return false;

	for (const r of results) {
		try {
			const hostname = new URL(r.link).hostname.toLowerCase();
			if (keywords.some((kw) => hostname.includes(kw))) return true;
		} catch { /* skip invalid */ }
	}
	return false;
}

function filterSearchResults(results: SearchResult[]): SearchResult[] {
	return results.filter((r) => {
		const url = r.link;
		return !EXCLUDED_DOMAINS.some((domain) => url.includes(domain));
	});
}

async function findHomepageWithOpenAi(
	ctx: IExecuteFunctions,
	companyQuery: string,
	results: SearchResult[],
	openAiKey: string,
	model: string,
): Promise<string | null> {
	const urlList = results
		.map((r) => `title: "${r.title}"\nurl: ${r.link}`)
		.join('\n\n');

	try {
		const response = await ctx.helpers.httpRequest({
			method: 'POST',
			url: 'https://api.openai.com/v1/chat/completions',
			headers: {
				Authorization: `Bearer ${openAiKey}`,
				'Content-Type': 'application/json',
			},
			body: {
				model,
				temperature: 0,
				messages: [
					{
						role: 'system',
						content:
							'You are a web search result evaluation bot. You get a company name and a list of search results. Return ONLY the URL from the provided list that most likely belongs to the company\'s own website. Prefer the root/homepage URL if available, but any page on their own domain is fine. You MUST pick one of the given URLs — never invent or guess a URL. Return just the URL, nothing else.',
					},
					{
						role: 'user',
						content: `### Company:\n\n${companyQuery}\n\n### URLs:\n\n${urlList}`,
					},
				],
			},
			timeout: 15000,
		});

		const content = response?.choices?.[0]?.message?.content?.trim();
		if (!content) return null;

		// Extract URL from response
		let candidate: string | null = null;
		if (/^https?:\/\//i.test(content)) {
			candidate = content;
		} else {
			const urlMatch = content.match(/https?:\/\/[^\s"'<>]+/);
			candidate = urlMatch ? urlMatch[0] : null;
		}

		if (!candidate) return null;

		// Validate the URL is from the provided results (prevent hallucination)
		const knownUrls = new Set(results.map((r) => r.link));
		if (knownUrls.has(candidate)) return candidate;

		// Try matching by domain — OpenAI may have returned a slightly different path
		const candidateDomain = new URL(candidate).hostname;
		const domainMatch = results.find((r) => {
			try { return new URL(r.link).hostname === candidateDomain; } catch { return false; }
		});
		return domainMatch ? domainMatch.link : null;
	} catch {
		return null;
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Plausibility Check + OpenAI Enrichment
// ═══════════════════════════════════════════════════════════════════════════════

// Note: salutation is intentionally excluded — Phase 7 (deriveSalutations) handles it
// more efficiently via a lookup table + batched AI fallback.
const EXTRACTABLE_FIELDS: Record<string, string> = {
	companyName: 'Company or practice name (Firmenname / Praxisname)',
	title: 'Academic/professional title (e.g. Dr., Prof., Dr. med., Dr. med. dent.)',
	firstName: 'First name (Vorname)',
	lastName: 'Last name (Nachname)',
	emails: 'All email addresses found (as JSON array of strings)',
	phones: 'All phone numbers found (as JSON array of strings, E.164 or local format)',
	faxNumbers: 'All fax numbers found (as JSON array of strings)',
	mobileNumbers: 'All mobile numbers found (as JSON array of strings)',
	vatId: 'VAT ID / USt-IdNr (format: DE followed by 9 digits)',
	taxNumber: 'Tax number / Steuernummer',
	street: 'Street address with house number (Straße + Hausnummer)',
	postalCode: 'Postal code / Postleitzahl (4-5 digits for DACH region)',
	city: 'City name (Stadt / Ort)',
	registrationCourt: 'Registration court (Registergericht / Amtsgericht)',
	registrationNumber: 'Registration number (e.g. HRB 12345)',
	chamber: 'Professional chamber (Kammer, e.g. Zahnärztekammer)',
	supervisoryAuthority: 'Supervisory authority (Aufsichtsbehörde, e.g. KZV)',
	professionalTitle: 'Professional title / Berufsbezeichnung (e.g. Zahnarzt)',
	website: 'Website URL',
	managingDirector: 'Managing director (Geschäftsführer/in)',
};

const OPENAI_CONCURRENCY = 15;

const ARRAY_FIELDS = new Set(['emails', 'phones', 'faxNumbers', 'mobileNumbers']);

/** Returns true when the value looks like a valid DACH postal code (4-5 digits, sane range). */
function isValidPostalCode(value: string | null | undefined): boolean {
	if (!value) return false;
	const stripped = String(value).replace(/\s/g, '');
	if (!/^\d{4,5}$/.test(stripped)) return false;
	const num = parseInt(stripped, 10);
	return num >= 1000 && num <= 99999;
}

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
	const hasContact = data.emails.length > 0 || data.phones.length > 0 || data.mobileNumbers.length > 0;
	if (!hasContact) return false;

	// Company name sanity: reject nav/menu fragments
	if (data.companyName) {
		const suspicious = /^(Home|Menü|Menu|Startseite|Navigation|Kontakt|Cookie|Datenschutz|Skip|Zum Inhalt)/i;
		if (suspicious.test(data.companyName)) return false;
		if (data.companyName.length < 3) return false;
	}

	// Email sanity
	if (data.emails.length > 0 && !data.emails.some((e) => /^[\w.+-]+@[\w.-]+\.\w{2,}$/.test(e))) return false;

	// Phone sanity: at least 6 digits in at least one number
	if (data.phones.length > 0 && !data.phones.some((p) => p.replace(/\D/g, '').length >= 6)) return false;

	// Address consistency: if we have one part, we should have the other
	if ((data.postalCode && !data.city) || (data.city && !data.postalCode)) return false;

	return true;
}

/**
 * Cleans an AI-extracted companyName: decode entities, strip comment/SEO tails,
 * strip leading Herr/Frau + title salutations, then apply NAME_REJECT_RE.
 * Returns null if the cleaned value should be rejected.
 *
 * NOTE: defined here as a forward reference — NAME_REJECT_RE is declared later
 * in the file but both live in the same module scope.
 */
function sanitizeAiCompanyName(value: unknown): string | null {
	if (value == null) return null;
	let s = String(value);
	s = decodeHtmlEntities(s)
		.replace(/\s*-->\s*/g, ' ')
		.split(/\s*\|\s*/)[0]
		.split(/\s*-->\s*/)[0]
		.replace(/\s+/g, ' ')
		.trim();
	// Strip leading salutation + titles so "Herr Prof. Dr. X" becomes usable.
	s = s.replace(/^(?:Herr(?:n)?|Frau)\s+/i, '');
	if (s.length < 3 || s.length > 150) return null;
	if (NAME_REJECT_RE.test(s)) return null;
	if (looksLikeAddressString(s)) return null;
	return s;
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
			// Plausible → only enrich empty fields
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const d = data as any;
			const missing = Object.keys(EXTRACTABLE_FIELDS).filter((f) =>
				ARRAY_FIELDS.has(f) ? (d[f] as unknown[]).length === 0 : d[f] === null || d[f] === undefined,
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
						if (field === 'postalCode' && !isValidPostalCode(String(extracted[field]))) continue;
						if (field === 'companyName') {
							const cleaned = sanitizeAiCompanyName(extracted[field]);
							if (cleaned === null) continue;
							data[field] = cleaned;
							continue;
						}
						if (ARRAY_FIELDS.has(field)) {
							let arr = Array.isArray(extracted[field])
								? (extracted[field] as string[]).map(String)
								: [String(extracted[field])];
							if (field === 'emails') arr = arr.filter((e) => !isPlaceholderEmail(e.toLowerCase()));
							data[field] = arr;
						} else {
							data[field] = String(extracted[field]);
						}
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

				// Only fill empty fields — never override regex results
				for (const field of missingFields) {
					if (extracted[field] != null && extracted[field] !== '') {
						if (field === 'postalCode' && !isValidPostalCode(String(extracted[field]))) continue;
						if (field === 'companyName' && data[field] === null) {
							const cleaned = sanitizeAiCompanyName(extracted[field]);
							if (cleaned !== null) data[field] = cleaned;
							continue;
						}
						if (ARRAY_FIELDS.has(field) && (data[field] as unknown[]).length === 0) {
							let arr = Array.isArray(extracted[field])
								? (extracted[field] as string[]).map(String)
								: [String(extracted[field])];
							if (field === 'emails') arr = arr.filter((e) => !isPlaceholderEmail(e.toLowerCase()));
							data[field] = arr;
						} else if (!ARRAY_FIELDS.has(field) && data[field] === null) {
							data[field] = String(extracted[field]);
						}
					}
				}
			}
		}
	}
}

/**
 * Phase 6b: focused person-recovery. For jobs where the scraper has a usable
 * companyName but no lastName, call OpenAI with a tight prompt asking
 * specifically for the primary responsible natural person. Only *fills* null
 * fields — never overrides existing values. Runs with the same concurrency
 * cap as the main enrichment.
 */
async function recoverMissingPersons(
	ctx: IExecuteFunctions,
	results: Array<{ data: ImpressumResult; text: string }>,
	openAiKey: string,
	model: string,
): Promise<void> {
	const targets: number[] = [];
	for (let i = 0; i < results.length; i++) {
		const data = results[i].data;
		const companyOk =
			!!data.companyName &&
			data.companyName.length >= 3 &&
			data.companyName.length <= 150 &&
			!NAME_REJECT_RE.test(data.companyName);
		const personMissing = !data.lastName || data.lastName.trim().length < 2;
		if (companyOk && personMissing) targets.push(i);
	}
	if (targets.length === 0) return;

	for (let i = 0; i < targets.length; i += OPENAI_CONCURRENCY) {
		const batch = targets.slice(i, i + OPENAI_CONCURRENCY);
		const responses = await Promise.allSettled(
			batch.map((idx) =>
				callPersonExtraction(
					ctx,
					results[idx].text,
					results[idx].data.companyName || '',
					openAiKey,
					model,
				),
			),
		);
		for (let j = 0; j < batch.length; j++) {
			const resp = responses[j];
			if (resp.status !== 'fulfilled' || !resp.value) continue;
			const idx = batch[j];
			const data = results[idx].data;
			const p = resp.value;

			const candidate: { firstName: string | null; lastName: string | null; salutation: string | null; title: string | null } = {
				firstName: typeof p.firstName === 'string' && p.firstName.trim() ? p.firstName.trim() : null,
				lastName: typeof p.lastName === 'string' && p.lastName.trim() ? p.lastName.trim() : null,
				salutation: typeof p.salutation === 'string' && p.salutation.trim() ? p.salutation.trim() : null,
				title: typeof p.title === 'string' && p.title.trim() ? p.title.trim() : null,
			};

			// Sanity-check the AI output with the same filters as the regex path.
			if (isRejectedPerson(candidate.firstName, candidate.lastName)) continue;
			if (!candidate.firstName && !candidate.lastName) continue;

			// Only fill nulls — never override.
			if (!data.lastName && candidate.lastName) data.lastName = candidate.lastName;
			if (!data.firstName && candidate.firstName) data.firstName = candidate.firstName;
			if (!data.salutation && candidate.salutation) {
				const s = candidate.salutation.replace(/^Herrn$/i, 'Herr');
				if (/^(Herr|Frau)$/i.test(s)) data.salutation = s;
			}
			if (!data.title && candidate.title) data.title = candidate.title;
		}
	}
}

async function callPersonExtraction(
	ctx: IExecuteFunctions,
	impressumText: string,
	companyName: string,
	apiKey: string,
	model: string,
): Promise<Record<string, unknown> | null> {
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
							'You are given the Impressum/legal-notice text of a German business. Identify the single primary responsible natural person for this Impressum — typically whoever appears as Inhaber, Geschäftsführer, Praxisinhaber, or under "Vertreten durch" / "Verantwortlich für den Inhalt". ' +
							'If two people are listed as equal co-managers (e.g. "Vertreten durch: Dr. Müller und Dr. Schmidt"), return the first one. ' +
							'If no single responsible natural person can be identified (pure corporate imprint with no named Geschäftsführer, or a multi-partner GbR without single managing partner), return {}. ' +
							'Never return streets, addresses, cities, titles, or placeholder text. Never guess. ' +
							'Return JSON with these optional fields: ' +
							'firstName (given name, e.g. "Marc"), lastName (family name, e.g. "Müller"), ' +
							'salutation ("Herr" or "Frau"), title (academic titles only, space-separated, e.g. "Dr. med. dent.").',
					},
					{
						role: 'user',
						content: `Company: ${companyName}\n\nImpressum text:\n---\n${truncated}\n---`,
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

async function callOpenAi(
	ctx: IExecuteFunctions,
	impressumText: string,
	fields: string[],
	apiKey: string,
	model: string,
): Promise<Record<string, unknown> | null> {
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
							'You extract structured data from German Impressum (legal notice) texts. Return a JSON object with ONLY the fields you can confidently identify in the text. Do NOT guess, invent, or hallucinate values. If a field is not clearly present in the text, omit it from the response. Fields marked as "array" must be returned as JSON arrays of strings, even if there is only one value.',
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
// Directory Homepage Link Extraction
// ═══════════════════════════════════════════════════════════════════════════════

function extractHomepageLinkFromDirectory(html: string, directoryUrl: string): string | null {
	let directoryHostname: string;
	try {
		directoryHostname = new URL(directoryUrl).hostname;
	} catch { return null; }

	const candidates: Array<{ url: string; score: number }> = [];

	const isExcludedHost = (hostname: string): boolean =>
		EXCLUDED_DOMAINS.some((d) => hostname.includes(d));

	// Strategy 1: <a> tags with "website"/"homepage"/"webseite" in text, aria-label, or title
	const linkRegex = /<a\s[^>]*href\s*=\s*["']([^"'#]+)["'][^>]*>([\s\S]*?)<\/a>/gi;
	let match;
	while ((match = linkRegex.exec(html)) !== null) {
		const href = match[1].trim();
		const fullTag = match[0];
		// Guard against non-greedy capture spanning huge HTML (missing/distant </a>) —
		// calling .replace() on multi-MB strings can overflow V8's regex stack.
		if (match[2].length > 5000) continue;
		const linkText = match[2].replace(/<[^>]+>/g, '').trim();

		if (href.startsWith('#') || href.startsWith('javascript:') || href.startsWith('mailto:')) continue;
		try {
			const resolved = new URL(href, directoryUrl);
			if (resolved.hostname === directoryHostname) continue;
			if (isExcludedHost(resolved.hostname)) continue;

			// Primary signal: link text or attributes must indicate "website"
			let primaryScore = 0;
			const textLower = linkText.toLowerCase();

			if (/\b(zur\s+website|zur\s+homepage|webseite\s+besuchen|website\s+besuchen|visit\s+website)\b/i.test(textLower)) primaryScore = 15;
			else if (/\b(website|webseite|homepage|internetseite)\b/i.test(textLower)) primaryScore = 10;

			if (/(?:aria-label|title)\s*=\s*["'][^"']*(?:website|webseite|homepage)[^"']*["']/i.test(fullTag)) primaryScore = Math.max(primaryScore, 10);

			// itemprop="url" on directory pages is a strong signal for the company's website
			if (/itemprop\s*=\s*["']url["']/i.test(fullTag)) primaryScore = Math.max(primaryScore, 12);

			// Only consider links that have a primary "website" signal
			if (primaryScore > 0) {
				let bonus = 0;
				if (/target\s*=\s*["']_blank["']/i.test(fullTag)) bonus += 3;
				if (/rel\s*=\s*["'][^"']*nofollow[^"']*["']/i.test(fullTag)) bonus += 2;
				if (/^https?:\/\//i.test(href)) bonus += 3;
				try {
					if (new URL(href, directoryUrl).pathname === '/') bonus += 5;
				} catch { /* skip */ }

				candidates.push({ url: resolved.href, score: primaryScore + bonus });
			}
		} catch { /* skip invalid */ }
	}

	// Strategy 2: schema.org structured data (JSON-LD, microdata)
	const schemaMatches = html.matchAll(/"(?:url|website|sameAs)"\s*:\s*"(https?:\/\/[^"]+)"/gi);
	for (const m of schemaMatches) {
		try {
			const u = new URL(m[1]);
			if (u.hostname !== directoryHostname && !isExcludedHost(u.hostname)) {
				candidates.push({ url: u.href, score: 8 });
			}
		} catch { /* skip */ }
	}

	// Strategy 3: Plain text patterns like "Website: www.example.de"
	const text = htmlToText(html);
	const textPattern = /(?:Website|Webseite|Homepage|Internet)\s*[:]\s*((?:https?:\/\/)?(?:www\.)?[\w.-]+\.\w{2,}(?:\/\S*)?)/gi;
	let textMatch;
	while ((textMatch = textPattern.exec(text)) !== null) {
		let url = textMatch[1].trim();
		if (!/^https?:\/\//i.test(url)) url = 'https://' + url;
		try {
			const u = new URL(url);
			if (u.hostname !== directoryHostname && !isExcludedHost(u.hostname)) {
				candidates.push({ url: u.href, score: 7 });
			}
		} catch { /* skip */ }
	}

	candidates.sort((a, b) => b.score - a.score);
	return candidates.length > 0 ? candidates[0].url : null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fallback URL Finding (contact, about, legal pages)
// ═══════════════════════════════════════════════════════════════════════════════

function findFallbackUrls(html: string, baseUrl: string): string[] {
	const found = new Set<string>();
	const linkRegex = /<a\s[^>]*href\s*=\s*["']([^"'#]+)["'][^>]*>([\s\S]*?)<\/a>/gi;
	let match;

	const FALLBACK_HREF = /\/(contact|kontakt|about|about-us|ueber-uns|legal|legal-notice|disclaimer|privacy|datenschutz)\b/i;
	const FALLBACK_TEXT = /\b(contact|kontakt|about\s*us|über\s*uns|legal|disclaimer|datenschutz)\b/i;

	while ((match = linkRegex.exec(html)) !== null) {
		const href = match[1].trim();
		if (match[2].length > 5000) continue;
		const linkText = match[2].replace(/<[^>]+>/g, '').trim();

		if (FALLBACK_HREF.test(href) || FALLBACK_TEXT.test(linkText)) {
			try {
				const resolved = new URL(href, baseUrl);
				// Only same-origin links
				if (resolved.origin === new URL(baseUrl).origin) {
					found.add(resolved.href);
				}
			} catch { /* skip invalid */ }
		}
	}

	return [...found];
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
		if (match[2].length > 5000) continue;
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

/**
 * Removes all occurrences of <tagName ...>...</tagName> from the string.
 * Uses iterative indexOf instead of [\s\S]*? regex to avoid V8 stack overflow
 * on very large HTML strings (pages with huge inline SVGs, base64 data, etc.).
 */
function stripTagBlocks(text: string, tagName: string): string {
	const open = `<${tagName}`;
	const close = `</${tagName}>`;
	const lower = text.toLowerCase();
	let result = '';
	let pos = 0;

	while (pos < text.length) {
		const start = lower.indexOf(open, pos);
		if (start === -1) {
			result += text.substring(pos);
			break;
		}
		result += text.substring(pos, start);
		const end = lower.indexOf(close, start);
		if (end === -1) {
			// No closing tag — discard rest
			break;
		}
		pos = end + close.length;
	}
	return result;
}

function htmlToText(html: string): string {
	// Cap input size — pages with huge inline SVGs/base64 can exceed V8 regex stack limits
	let text = html.length > 2_000_000 ? html.substring(0, 2_000_000) : html;
	text = stripTagBlocks(text, 'script');
	text = stripTagBlocks(text, 'style');
	text = stripTagBlocks(text, 'noscript');
	text = text.replace(
		/<\/(?:p|div|h[1-6]|li|tr|section|article|header|footer|main|aside|nav)>/gi,
		'\n',
	);
	text = text.replace(/<br\s*\/?>/gi, '\n');
	text = text.replace(/<\/(?:td|th)>/gi, ' ');
	// Preserve alt/title text from img tags before stripping (sites use <img alt="Fax"> icons)
	text = text.replace(/<img\s[^>]*?\balt\s*=\s*["']([^"']*)["'][^>]*>/gi, ' $1 ');
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
// Result Sanitization
// ═══════════════════════════════════════════════════════════════════════════════

const ENTITY_MAP: Record<string, string> = {
	'&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', '&apos;': "'",
	'&nbsp;': ' ', '&shy;': '', '&sect;': '§', '&reg;': '®', '&copy;': '©',
	'&trade;': '™', '&times;': '×', '&middot;': '·', '&bull;': '•',
	'&laquo;': '«', '&raquo;': '»', '&ldquo;': '"', '&rdquo;': '"',
	'&lsquo;': "'", '&rsquo;': "'", '&ndash;': '–', '&mdash;': '—',
	'&hellip;': '…', '&deg;': '°',
	'&ouml;': 'ö', '&auml;': 'ä', '&uuml;': 'ü',
	'&Ouml;': 'Ö', '&Auml;': 'Ä', '&Uuml;': 'Ü', '&szlig;': 'ß',
	'&eacute;': 'é', '&Eacute;': 'É', '&egrave;': 'è', '&Egrave;': 'È',
	'&ecirc;': 'ê', '&agrave;': 'à', '&ccedil;': 'ç',
	'&uacute;': 'ú', '&aacute;': 'á', '&iacute;': 'í', '&oacute;': 'ó',
};

function decodeHtmlEntities(s: string): string {
	let out = s;
	for (const [ent, ch] of Object.entries(ENTITY_MAP)) out = out.split(ent).join(ch);
	out = out.replace(/&#(\d+);/g, (_, d: string) => String.fromCharCode(parseInt(d, 10)));
	out = out.replace(/&#x([0-9a-fA-F]+);/g, (_, h: string) => String.fromCharCode(parseInt(h, 16)));
	return out;
}

function sanitizeString(value: string): string {
	return decodeHtmlEntities(value)
		.replace(/\x00/g, '')                      // null bytes
		.replace(/\\u[0-9a-fA-F]{4}/g, '')         // unicode escape sequences
		.replace(/\\/g, '')                        // stray backslashes
		.replace(/\s*-->\s*/g, ' ')                // HTML-comment leak markers ("--> --> -->")
		.replace(/\s{2,}/g, ' ')
		.trim();
}

function sanitizeResult(data: ImpressumResult): void {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const d = data as any;
	for (const key of Object.keys(d)) {
		const val = d[key];
		if (typeof val === 'string') {
			d[key] = sanitizeString(val);
		} else if (Array.isArray(val)) {
			d[key] = val.map((v: unknown) => typeof v === 'string' ? sanitizeString(v) : v);
		}
	}
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
	emails: string[];
	phones: string[];
	faxNumbers: string[];
	mobileNumbers: string[];
	vatId: string | null;
	taxNumber: string | null;
	street: string | null;
	postalCode: string | null;
	city: string | null;
	country: string | null;
	countryCode: string | null;
	registrationCourt: string | null;
	registrationNumber: string | null;
	chamber: string | null;
	supervisoryAuthority: string | null;
	professionalTitle: string | null;
	website: string | null;
	managingDirector: string | null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Salutation Derivation from First Name (lookup table + AI fallback)
// ═══════════════════════════════════════════════════════════════════════════════

// Common German first names → salutation. Covers the vast majority of cases.
const MALE_NAMES = new Set([
	'alexander', 'andreas', 'anton', 'axel', 'benjamin', 'bernd', 'bernhard',
	'björn', 'boris', 'carsten', 'christian', 'christoph', 'claus', 'clemens',
	'daniel', 'david', 'dennis', 'detlef', 'dieter', 'dietmar', 'dirk',
	'dominik', 'eckhard', 'edgar', 'eric', 'erik', 'ernst', 'fabian',
	'felix', 'florian', 'frank', 'franz', 'frederic', 'friedrich', 'georg',
	'gerald', 'gerd', 'gerhard', 'gregor', 'günter', 'günther', 'guido',
	'hans', 'harald', 'heiko', 'heinz', 'helmut', 'hendrik', 'henning',
	'herbert', 'hermann', 'holger', 'horst', 'ingo', 'jan', 'jens', 'joachim',
	'jochen', 'joerg', 'johann', 'johannes', 'jonas', 'jörg', 'josef',
	'jürgen', 'kai', 'karl', 'karsten', 'klaus', 'konrad', 'lars',
	'lorenz', 'lothar', 'lukas', 'lutz', 'manfred', 'marc', 'marcel',
	'marco', 'marcus', 'mario', 'markus', 'martin', 'mathias', 'matthias',
	'max', 'maximilian', 'michael', 'moritz', 'nico', 'niklas', 'nils',
	'norbert', 'olaf', 'oliver', 'otto', 'pascal', 'patrick', 'paul',
	'peter', 'philipp', 'rainer', 'ralf', 'ralph', 'reinhard', 'robert',
	'robin', 'roland', 'rolf', 'roman', 'rüdiger', 'sascha', 'sebastian',
	'simon', 'stefan', 'steffen', 'stephan', 'sven', 'thomas', 'thorsten',
	'tim', 'tobias', 'torsten', 'uwe', 'volker', 'walter', 'werner',
	'wilhelm', 'willi', 'wolfgang',
]);

const FEMALE_NAMES = new Set([
	'alexandra', 'andrea', 'anja', 'anna', 'annett', 'annette', 'antje',
	'astrid', 'barbara', 'beate', 'bianca', 'birgit', 'britta', 'carla',
	'carmen', 'caroline', 'charlotte', 'christa', 'christiane', 'christina',
	'claudia', 'constanze', 'cordula', 'cornelia', 'dagmar', 'daniela',
	'diana', 'doris', 'dorothea', 'edith', 'elke', 'ellen', 'emma',
	'eva', 'franziska', 'gabriele', 'gisela', 'gudrun', 'hannelore',
	'heide', 'heike', 'helga', 'ina', 'ines', 'ingrid', 'irene', 'iris',
	'jana', 'janina', 'jasmin', 'jennifer', 'jessica', 'julia', 'juliane',
	'karen', 'karin', 'karola', 'katharina', 'kathrin', 'katja', 'katrin',
	'kerstin', 'klara', 'lara', 'laura', 'lea', 'lena', 'lisa', 'luise',
	'manuela', 'margarete', 'maria', 'marie', 'marina', 'marion', 'marlene',
	'martina', 'meike', 'melanie', 'michaela', 'monika', 'nadine', 'nadja',
	'nicole', 'nina', 'patricia', 'petra', 'pia', 'regina', 'renate',
	'ruth', 'sabina', 'sabine', 'sandra', 'sara', 'sarah', 'silke',
	'simone', 'sonja', 'sophia', 'sophie', 'stefanie', 'stephanie',
	'susanne', 'svenja', 'tanja', 'tatjana', 'ulrike', 'ursula', 'ute',
	'vanessa', 'vera', 'verena', 'veronika',
]);

function lookupSalutation(firstName: string): 'Herr' | 'Frau' | null {
	const lower = firstName.toLowerCase().trim();
	if (MALE_NAMES.has(lower)) return 'Herr';
	if (FEMALE_NAMES.has(lower)) return 'Frau';
	return null;
}

/**
 * Derives salutations from firstNames: lookup table first, AI fallback for unknowns.
 */
async function deriveSalutations(
	ctx: IExecuteFunctions,
	results: Array<{ job: ScrapeJob; data: ImpressumResult; text: string }>,
	openAiKey: string,
	model: string,
): Promise<void> {
	const needsAi: Array<{ idx: number; firstName: string }> = [];

	for (let i = 0; i < results.length; i++) {
		const { data } = results[i];
		if (!data.salutation && data.firstName) {
			const lookup = lookupSalutation(data.firstName);
			if (lookup) {
				data.salutation = lookup;
			} else {
				needsAi.push({ idx: i, firstName: data.firstName });
			}
		}
	}

	if (needsAi.length === 0) return;

	const nameList = needsAi.map((n) => n.firstName).join(', ');

	try {
		const response = await ctx.helpers.httpRequest({
			method: 'POST',
			url: 'https://api.openai.com/v1/chat/completions',
			headers: {
				Authorization: `Bearer ${openAiKey}`,
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
							'For each given first name, determine the German salutation: "Herr" (male) or "Frau" (female). Return a JSON object mapping each name to its salutation. If unsure, omit the name.',
					},
					{
						role: 'user',
						content: nameList,
					},
				],
			},
			timeout: 15000,
		});

		const content = response?.choices?.[0]?.message?.content;
		if (!content) return;

		const mapping: Record<string, string> = JSON.parse(content);

		for (const { idx, firstName } of needsAi) {
			const val = mapping[firstName];
			if (val === 'Herr' || val === 'Frau') {
				results[idx].data.salutation = val;
			}
		}
	} catch {
		// Salutation derivation is best-effort
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Phone Number Normalization (regex + AI fallback)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Tries to normalize a single phone number string with regex.
 * Returns an array of normalized numbers, or null if the format is too complex
 * (compound numbers with "/" that need AI interpretation).
 */
function normalizePhoneLocal(raw: string): string[] | null {
	const trimmed = raw.trim();

	// Detect compound numbers like "0123 456 / 789" — needs AI to split correctly
	if (/\d\s*[/]\s*\d/.test(trimmed)) return null;

	// Strip parentheses, normalize whitespace/dashes to single space
	let cleaned = trimmed
		.replace(/[()]/g, '')
		.replace(/[-–—.]+/g, ' ')
		.replace(/\s+/g, ' ')
		.trim();

	// Already in international format (+49 ...)? Just clean up spacing
	if (/^\+\d/.test(cleaned)) {
		// Collapse to digits after the +, then format as "+XX XXXX XXXXXXX"
		const digits = cleaned.replace(/[^\d+]/g, '');
		if (digits.replace(/\D/g, '').length >= 6) return [digits];
		return null;
	}

	// Starts with 00XX (international dialing)? Convert to +XX
	if (/^00\d/.test(cleaned)) {
		cleaned = '+' + cleaned.slice(2).replace(/\s/g, '');
		if (cleaned.replace(/\D/g, '').length >= 6) return [cleaned];
		return null;
	}

	// Standard German number: starts with 0, has enough digits
	const digits = cleaned.replace(/\D/g, '');
	if (/^0\d/.test(digits) && digits.length >= 6 && digits.length <= 15) {
		// Format as "0XXXX XXXXXXX" — split area code (2-5 digits after 0) from subscriber
		// Simple approach: keep the cleaned version with normalized spacing
		const formatted = cleaned.replace(/\s{2,}/g, ' ');
		return [formatted];
	}

	// Doesn't look like a standard number — let AI handle it
	if (digits.length < 6) return null;
	return [cleaned];
}

/**
 * Normalizes phone numbers: regex for simple formats, AI fallback for complex ones.
 */
async function normalizePhoneNumbers(
	ctx: IExecuteFunctions,
	results: Array<{ job: ScrapeJob; data: ImpressumResult; text: string }>,
	openAiKey: string,
	model: string,
): Promise<void> {
	type PhoneEntry = { resultIdx: number; field: 'phones' | 'faxNumbers' | 'mobileNumbers'; raw: string };
	const allEntries: PhoneEntry[] = [];
	const needsAi: Array<{ entryIdx: number } & PhoneEntry> = [];

	for (let i = 0; i < results.length; i++) {
		const { data } = results[i];
		for (const num of data.phones) allEntries.push({ resultIdx: i, field: 'phones', raw: num });
		for (const num of data.faxNumbers) allEntries.push({ resultIdx: i, field: 'faxNumbers', raw: num });
		for (const num of data.mobileNumbers) allEntries.push({ resultIdx: i, field: 'mobileNumbers', raw: num });
	}

	if (allEntries.length === 0) return;

	// Step 1: Try regex normalization for each number
	const resolved: Array<string[] | null> = allEntries.map((e) => normalizePhoneLocal(e.raw));

	for (let i = 0; i < resolved.length; i++) {
		if (resolved[i] === null) {
			needsAi.push({ entryIdx: i, ...allEntries[i] });
		}
	}

	// Step 2: AI fallback only for numbers that regex couldn't handle
	if (needsAi.length > 0) {
		const numberList = needsAi.map((n, i) => `${i}: ${n.raw}`).join('\n');

		try {
			const response = await ctx.helpers.httpRequest({
				method: 'POST',
				url: 'https://api.openai.com/v1/chat/completions',
				headers: {
					Authorization: `Bearer ${openAiKey}`,
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
								'You normalize German phone numbers. For each numbered entry:\n' +
								'1. Split entries that contain multiple numbers (e.g. "23123 43 / 44" means two numbers sharing a prefix: the base number ending in 43 and another ending in 44).\n' +
								'2. Format each as a clean, readable German phone number (e.g. "0124 2312343").\n' +
								'3. Return a JSON object where each key is the entry index (as string) and the value is an array of normalized phone numbers.',
						},
						{
							role: 'user',
							content: numberList,
						},
					],
				},
				timeout: 15000,
			});

			const content = response?.choices?.[0]?.message?.content;
			if (content) {
				const mapping: Record<string, string[]> = JSON.parse(content);
				for (let i = 0; i < needsAi.length; i++) {
					const aiResult = mapping[String(i)];
					if (Array.isArray(aiResult)) {
						resolved[needsAi[i].entryIdx] = aiResult.filter((n) => typeof n === 'string' && n.length > 0);
					}
				}
			}
		} catch {
			// AI normalization is best-effort
		}
	}

	// Step 3: Rebuild arrays from resolved values
	for (const result of results) {
		result.data.phones = [];
		result.data.faxNumbers = [];
		result.data.mobileNumbers = [];
	}

	for (let i = 0; i < allEntries.length; i++) {
		const { resultIdx, field, raw } = allEntries[i];
		const nums = resolved[i];
		if (nums && nums.length > 0) {
			results[resultIdx].data[field].push(...nums);
		} else {
			// Keep original if nothing worked
			results[resultIdx].data[field].push(raw);
		}
	}
}

const COUNTRY_CODE_BY_TLD: Record<string, string> = {
	de: 'DE', at: 'AT', ch: 'CH', li: 'LI', lu: 'LU',
};

const COUNTRY_NAME_BY_CODE: Record<string, string> = {
	DE: 'Deutschland', AT: 'Österreich', CH: 'Schweiz', LI: 'Liechtenstein', LU: 'Luxemburg',
};

const COUNTRY_CODE_BY_PHONE_PREFIX: Array<[string, string]> = [
	['+49', 'DE'], ['+43', 'AT'], ['+41', 'CH'], ['+423', 'LI'], ['+352', 'LU'],
	['0049', 'DE'], ['0043', 'AT'], ['0041', 'CH'],
];

function deriveCountryCode(
	siteDomain: string | undefined,
	phones: string[],
	postalCode: string | null,
	vatId: string | null,
): string {
	// 1. TLD
	if (siteDomain) {
		const tld = siteDomain.split('.').pop()?.toLowerCase() || '';
		if (COUNTRY_CODE_BY_TLD[tld]) return COUNTRY_CODE_BY_TLD[tld];
	}

	// 2. VAT ID prefix
	if (vatId) {
		const prefix = vatId.replace(/\s/g, '').substring(0, 2).toUpperCase();
		if (COUNTRY_NAME_BY_CODE[prefix]) return prefix;
	}

	// 3. Phone prefix
	for (const phone of phones) {
		const cleaned = phone.replace(/[\s\-()]/g, '');
		for (const [prefix, code] of COUNTRY_CODE_BY_PHONE_PREFIX) {
			if (cleaned.startsWith(prefix)) return code;
		}
	}

	// 4. Postal code pattern
	if (postalCode) {
		const pc = postalCode.replace(/\s/g, '');
		if (/^\d{5}$/.test(pc)) return 'DE';
		if (/^\d{4}$/.test(pc)) {
			const num = parseInt(pc, 10);
			if (num >= 1000 && num <= 9999) {
				// Austrian codes: 1010-9992; Swiss codes: 1000-9658
				// Overlap exists, but Austrian codes >= 1010 with leading 1-9 are common
				// Swiss codes never start with 0; use VAT/phone as tiebreaker above
				// Without better signal, prefer CH for 1xxx-4xxx, AT for 5xxx-9xxx
				if (num >= 5000) return 'AT';
				return 'CH';
			}
		}
	}

	// 5. Default
	return 'DE';
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

	let siteDomain: string | undefined;
	try {
		siteDomain = new URL(sourceUrl || impressumUrl).hostname.replace(/^www\./, '');
	} catch { /* ignore */ }

	const phones = extractPhones(business);
	const vatId = extractVatId(section);
	const countryCode = deriveCountryCode(siteDomain, phones, address.postalCode, vatId);

	return {
		sourceUrl,
		impressumUrl,
		companyName: extractCompanyName(business),
		salutation: person.salutation,
		title: person.title,
		firstName: person.firstName,
		lastName: person.lastName,
		emails: extractEmails(html, business, siteDomain),
		phones,
		faxNumbers: extractFaxNumbers(business),
		mobileNumbers: extractMobileNumbers(business),
		vatId,
		taxNumber: extractTaxNumber(section),
		street: address.street,
		postalCode: address.postalCode,
		city: address.city,
		country: COUNTRY_NAME_BY_CODE[countryCode] || countryCode,
		countryCode,
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

// Prefixes that indicate the "companyName" we'd extract is actually a
// section heading, legal-text intro, nav label, template placeholder or
// HTML fragment. Final output matching this is discarded.
const NAME_REJECT_RE =
	/^(?:Haftung|Seitenbetreiber|Betreiber|Start\b|Startseite|Home\b|Hauptmen[üu]|Seiten\b|Facebook\b|Imprint\b|Legal\s+Notice|Nutzungsbedingungen|Anbieterkennzeichnung|Anbieter\b|Inhaltlich\s+Verantwortlich|Informationspflicht|Information\s+gem[äa][ßs]|Medieninhaber|Herausgeber|Anschrift\b|Hier\s+finden\s+Sie|oder\s+via|Die\s+Internetseite|Der\s+gesamte|Betreuung\s+der|Volltextsuche|Allgemeine\s+Liefer|GENDER-HINWEIS|Frau\s+[A-ZÄÖÜ]|Herr\s+[A-ZÄÖÜ]|Wir\s+bem[üu]hen|Mein\s+Team|\d+\.\s+[A-ZÄÖÜ]|OnePress\s+Theme|Theme\s+von|Design\s+(?:by|von)|Powered\s+(?:by|von)|©\s*\d{4}|\$event\b|\$\(|document\.|window\.|E\s+ma\s*il|⭐|☎|☰|→|↓|↑|»|«|\p{Extended_Pictographic})/iu;

/**
 * Secondary reject: long name that looks like it contains a full address
 * (e.g. "Zahnarztpraxis Arendt Sabine Arendt Dr.-Kurt-Fischer-Straße 10A 06888 Lutherstadt").
 * The parser slurped the whole 'name + street + postal_code + city' block.
 */
function looksLikeAddressString(name: string): boolean {
	if (name.length < 50) return false;
	// Contains street word followed by house number, OR PLZ-style 5-digit block
	const hasStreetNum = /\b\S+?(?:straße|strasse|weg|allee|ring|platz|gasse|damm|ufer|\bstr\.?)\s+\d/i.test(name);
	const hasPostalBlock = /\b\d{4,5}\s+[A-ZÄÖÜ][a-zäöüß]/.test(name);
	return hasStreetNum && hasPostalBlock;
}

// Matches an intro line with a colon-separated value, e.g.
// "Betreiber der Website: Zahnarztpraxis Dr. X" → captures "Zahnarztpraxis Dr. X".
const INTRO_LABEL_RE =
	/^(?:Seitenbetreiber(?:\s+i\.?\s*S\.?\s*d\.?\s*§?\s*\d+\s*TMG)?|Betreiber(?:\s+(?:der\s+Website|dieses\s+Internetauftrittes?))?|Anbieter(?:kennzeichnung[^:]*)?|Medieninhaber(?:\s*,\s*Herausgeber[^:]*)?|Herausgeber|Inhaltlich\s+Verantwortlich(?:er)?(?:\s+gem[äa][ßs][^:]*)?)\s*:\s*(.+)$/i;

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
		/^(?:Cookie|Akzeptieren|Einstellungen|Zustimmen|Ablehnen|Einverstanden|Datenschutzerklärung)/i,
		/^(?:Suche|Suchen|Anmelden|Login|Registrieren|Abmelden|Warenkorb)/i,
		/^(?:Öffnungszeiten|Sprechzeiten|Sprechstunden|Termine(?:\.?\s|$)|Online.?Termin)/i,
		/^(?:Über\s+uns|Unser\s+Team|Unsere\s+Praxis|Leistungen|Service(?:s)?(?:\s|$))/i,
		/^(?:Willkommen|Herzlich|Schön|Wir\s+freuen|Wir\s+begrüßen|Wir\s+bieten)/i,
		/^(?:Mo|Di|Mi|Do|Fr|Sa|So)[\s.,-]/,
		/^(?:Montag|Dienstag|Mittwoch|Donnerstag|Freitag|Samstag|Sonntag)/i,
		/^(?:Alle\s+Rechte|All\s+rights|Copyright\b)/i,
		/^(?:Zurück|Weiter|Seite\s+\d|Mehr\s+erfahren|Weiterlesen|Jetzt\s+)/i,
		NAME_REJECT_RE,
	];

	let startLine = 0;
	for (let i = 0; i < lines.length; i++) {
		if (/^(?:Impressum|Angaben\s+gemäß|Pflichtangaben|IMPRESSUM)/i.test(lines[i])) {
			startLine = i + 1;
			break;
		}
	}

	// Pre-scan: check for explicit company name labels (e.g. "Firma: X" or "Firmenname: X")
	const labelPattern = /^(?:Firma|Firmenname|Unternehmen|Betrieb|Name)\s*:\s*/i;
	for (let i = startLine; i < Math.min(startLine + 10, lines.length); i++) {
		const labelMatch = lines[i].match(labelPattern);
		if (!labelMatch) continue;
		const afterLabel = lines[i].substring(labelMatch[0].length).trim();
		// Inline value: "Firmenname: Schlosserei Weidenbach e.K."
		if (afterLabel.length >= 3) return afterLabel;
		// Label on its own line: next line is the company name
		if (i + 1 < lines.length && lines[i + 1].length >= 3 && lines[i + 1].length <= 150) {
			return lines[i + 1];
		}
	}

	// Intro-label pattern: "Betreiber der Website: Zahnarztpraxis Dr. X", "Seitenbetreiber: Y GmbH",
	// "Inhaltlich Verantwortlicher gemäß § 5 DDG: Viviane Schubert". Take the value side.
	for (let i = startLine; i < Math.min(startLine + 10, lines.length); i++) {
		const introMatch = lines[i].match(INTRO_LABEL_RE);
		if (!introMatch) continue;
		const value = introMatch[1].trim();
		if (value.length >= 3 && value.length <= 150 && !NAME_REJECT_RE.test(value)) return value;
	}

	// Collect and score candidates instead of returning the first match
	const candidates: Array<{ line: string; score: number }> = [];

	for (let i = startLine; i < Math.min(startLine + 8, lines.length); i++) {
		const line = lines[i];
		if (line.length < 5 || line.length > 150) continue;

		let skip = false;
		for (const p of skipPatterns) {
			if (p.test(line)) {
				skip = true;
				break;
			}
		}
		if (skip) continue;

		if (!/[a-zäöüß]/i.test(line)) continue;

		let score = 1;

		// Strong positive: legal form suffix — almost certainly a company name
		if (/(?:GmbH|GbR|e\.?\s?K\.?|OHG|KG|UG|PartG|Part\s?mbB|Partnerschaft|mbH|AG|e\.?\s?V\.?|Ltd\.?|Co\.)(?:\s|$)/i.test(line)) {
			score += 50;
		}

		// Strong positive: medical/dental practice keywords
		if (/(?:praxis|MVZ|Medizinisch|Zahnmedizin|Zahnarzt|Gemeinschaftspraxis|Praxisgemeinschaft|Zahnheilkunde|Kieferorthopäd|Zahntechni|Implantolog|Klinik\b|Zentrum\b)/i.test(line)) {
			score += 40;
		}

		// Medium positive: "Dr." in compound names (e.g. "Dr. Müller & Dr. Schmidt")
		if (/Dr\./i.test(line) && /&|und|,/.test(line)) {
			score += 20;
		}

		// Slight positive: earlier lines are more likely the company name (tiebreaker)
		score += Math.max(0, 5 - (i - startLine));

		// Negative: looks like a sentence (common verbs / pronouns)
		if (/\b(?:ist|sind|wird|werden|haben|hat|können|kann|sollen|wir|Sie|bitte|unsere[mnrs]?|diese[mnrs]?)\b/i.test(line)) {
			score -= 20;
		}

		// Negative: looks like a street address (contains house number pattern)
		if (/(?:str\.|straße|weg|allee|platz|gasse|ring|damm)\s+\d/i.test(line) || /\d+\s*[a-c]?\s*$/i.test(line)) {
			score -= 15;
		}

		candidates.push({ line, score });
	}

	if (candidates.length === 0) return null;

	candidates.sort((a, b) => b.score - a.score);
	const pick = candidates[0].line;
	// Final safety net: never return a heading/nav/legal-intro as companyName.
	// Also strip SEO spam tails ("Company Name | Some SEO blurb --> -->"), keep the first segment.
	const cleaned = pick.split(/\s*\|\s*/)[0].split(/\s*-->\s*/)[0].trim();
	if (cleaned.length < 3 || cleaned.length > 150) return null;
	if (NAME_REJECT_RE.test(cleaned)) return null;
	if (looksLikeAddressString(cleaned)) return null;
	return cleaned;
}

interface PersonInfo {
	salutation: string | null;
	title: string | null;
	firstName: string | null;
	lastName: string | null;
}

// Tokens that MUST NOT appear as firstName (German street prepositions + role words).
// An address line like "An der Krusau 7" mis-parsed as first="An" last="Krusau" triggers this.
const FIRST_NAME_REJECT = new Set([
	'Am', 'An', 'Auf', 'In', 'Bei', 'Zum', 'Zur', 'Hinter', 'Vor',
	'Unter', 'Über', 'Ueber', 'Neben', 'Der', 'Die', 'Das', 'Haupt',
	'Dortelweiler', 'Potsdamer', 'Brunsbütteler',
	'Stadt', 'Markt',
	'Geschäftsführer', 'Geschäftsführerin', 'Inhaber', 'Inhaberin',
	'Betreiber', 'Betreiberin', 'Herausgeber', 'Verantwortlich',
	'Vertreten', 'Leitung', 'Praxisleitung',
	'Physiotherapiepraxis', 'Zahnarztpraxis', 'Environmental', 'Impressum',
]);

// Tokens that MUST NOT appear as lastName — address suffixes, title fragments,
// generic field-labels that the parser slurped past the actual name.
const LAST_NAME_REJECT = new Set([
	'Dipl.', 'Dr.', 'Prof.', 'D.',
	'Platz', 'Straße', 'Str.', 'Weg', 'Allee', 'Ring', 'Gasse', 'Hof',
	'Bauhof', 'Markt', 'Anschrift',
	'LLC', 'Expert', 'Sanum',
	'Umsatzsteueridentifikationsnummer:', 'USt-IdNr.:', 'UStIdNr.:',
]);

// Token-level test: does this single word look like a company suffix rather
// than a personal name? Catches "Schmerzfreizentrum" / "Zahnzentrum" /
// "Physiotherapiepraxis" / "Therapiezentrum" etc. — all company words the
// AI sometimes confidently returns as firstName/lastName.
const COMPANY_WORD_TOKEN_RE =
	/(?:praxis|praxen|zentrum|klinik|kliniken|therapie|institut|apotheke|gesundheit|medizin|naturheilkunde|zahnheilkunde|kieferorthop[äa]die|ambulatorium|sanat(?:orium)|zahnarzt|zahnärzte|physio|implantologie|mvz|gbr|gmbh|(?:^|\s)ag$|kg|ohg|\bug\b|mbh|mbb|eV|partnerschaft|sellwerk)$/i;

function looksLikeCompanyWord(token: string): boolean {
	if (token.length < 3) return false;
	return COMPANY_WORD_TOKEN_RE.test(token);
}

function isRejectedPerson(first: string | null, last: string | null): boolean {
	if (first && FIRST_NAME_REJECT.has(first)) return true;
	if (last && LAST_NAME_REJECT.has(last)) return true;
	// Single-token company words sneaking in as first/last name.
	if (first && looksLikeCompanyWord(first)) return true;
	if (last && looksLikeCompanyWord(last)) return true;
	// Multi-word lastName whose FIRST token is a company word
	// (e.g. lastName "Zahnzentrum Zahnengel" — company name leaked into person).
	if (last) {
		const firstTok = last.split(/\s+/)[0];
		if (firstTok && looksLikeCompanyWord(firstTok)) return true;
	}
	// Both tokens missing any lowercase letters → probably ALL CAPS navigation/headings
	if (first && last && !/[a-zäöüß]/.test(first + last)) return true;
	return false;
}

// Tokens that separate company-speak from the real person name. A value like
// "Zahnarztpraxis am Schloss Köpenick Olaf Vogel" needs to skip the leading
// five tokens to reach the actual firstName.
const NON_NAME_TOKEN_RE =
	/^(?:Dr\.?|Prof\.?|Dipl\.?|M\.?\s*Sc\.?|B\.?\s*Sc\.?|M\.?\s*A\.?|med\.?|dent\.?|rer\.?|nat\.?|phil\.?|jur\.?|Stom\.?|Ing\.?|Kfm\.?|Psych\.?|GmbH|AG|KG|OHG|GbR|UG|mbH|mbb|e\.?K\.?|e\.?V\.?|Co\.?|Ltd\.?|Partnerschaft|Praxis\w*|Zahnarzt\w*|Zahn(?:ärzt|arzt)\w*|Kieferorthop[aä]d\w*|Gemeinschaftspraxis|Praxisgemeinschaft|Klinik\w*|Zentrum|Institut|Physiotherapie\w*|MVZ|Heilpraktiker|Dental\w*|Arztpraxis|Kinderzahnarztpraxis|Ordination|am|an|auf|in|bei|im|zur|zum|von|der|die|das|und|dem|des|Haus|Schloss|&)$/i;

const NAME_TOKEN_RE =
	/^(?:[A-ZÄÖÜ][a-zäöüß]+(?:-[A-ZÄÖÜ][a-zäöüß]+)?|[A-ZÄÖÜ]\.)$/;

/**
 * Parses a single candidate value (the string captured after a label like
 * "Vertreten durch:") into a PersonInfo. Returns null if nothing usable was
 * found. Skips leading company-speak so values that lead with a company name
 * still yield the buried person (common in small practices: "Zahnarztpraxis
 * Peter Veit" → Peter Veit).
 */
function parsePersonFromLabelValue(input: string): PersonInfo | null {
	const result: PersonInfo = { salutation: null, title: null, firstName: null, lastName: null };

	// Keep only the first person if multiple are listed with & / "und" / ","
	let s = input
		.split(/\s*(?:&|\s+und\s+)\s*/)[0].trim()
		.split(/\s*,\s*/)[0].trim()
		.replace(/\s*\([^)]*\)/g, '')
		.replace(/\s*[-–]\s+\S.*$/, '')
		.trim();
	if (!s) return null;

	// Salutation prefix
	const salu = s.match(/^(Herr(?:n)?|Frau)\s+/i);
	if (salu) {
		result.salutation = salu[1].replace(/^Herrn$/i, 'Herr');
		s = s.substring(salu[0].length).trim();
	}

	// Title prefixes (may stack)
	const titles: string[] = [];
	let advanced = true;
	while (advanced) {
		advanced = false;
		const titlePatterns = [
			/^Prof\.?\s*/i,
			/^Dr\.?\s*(?:med\.?\s*(?:dent\.?\s*)?|rer\.?\s*nat\.?\s*|phil\.?\s*|jur\.?\s*|h\.?\s*c\.?\s*)?/i,
			/^Dipl\.?\s*-?\s*(?:Stom\.?|Med\.?|Ing\.?|Kfm\.?|Kff\.?|Wirt\w*\.?|Psych\.?|Päd\.?|Volksw\w*\.?|Betriebsw\w*\.?)\s*/i,
			/^M\.?\s*Sc\.?\s*/i,
			/^B\.?\s*Sc\.?\s*/i,
			/^M\.?\s*A\.?\s*/i,
		];
		for (const re of titlePatterns) {
			const m = s.match(re);
			if (m) { titles.push(m[0].trim()); s = s.substring(m[0].length).trim(); advanced = true; break; }
		}
	}
	if (titles.length) result.title = titles.join(' ');

	// Tokenize and skip leading non-name tokens (company speak, residual titles)
	let tokens = s.split(/\s+/).filter((t) => t.length > 0);
	while (tokens.length > 0 && NON_NAME_TOKEN_RE.test(tokens[0])) tokens.shift();
	// After skipping, also skip tokens that aren't plausible name tokens
	while (tokens.length > 0 && !NAME_TOKEN_RE.test(tokens[0])) tokens.shift();

	if (tokens.length === 0) return null;

	// Prefer first two consecutive name tokens (firstName + lastName)
	if (tokens.length >= 2 && NAME_TOKEN_RE.test(tokens[0]) && NAME_TOKEN_RE.test(tokens[1])) {
		// Reject single-letter-with-dot as firstName (it's an initial — lastName will be next token)
		if (/^[A-ZÄÖÜ]\.$/.test(tokens[0]) && NAME_TOKEN_RE.test(tokens[1]) && !/^[A-ZÄÖÜ]\.$/.test(tokens[1])) {
			// "P. Müller" — treat as initialed firstName
			result.firstName = tokens[0];
			result.lastName = tokens[1];
		} else {
			result.firstName = tokens[0];
			// Allow a hyphenated lastName to span two tokens if joined with dash
			result.lastName = tokens[1];
		}
		return result;
	}

	// Single token → lastName only
	result.lastName = tokens[0];
	return result;
}

function extractPersonName(businessText: string): PersonInfo {
	const empty: PersonInfo = { salutation: null, title: null, firstName: null, lastName: null };

	// Label-based candidates. Ordered most → least specific. Colon is optional —
	// some impressums write "Vertreten durch Rainer Gleß" without punctuation.
	// Value is everything up to the next newline.
	const labelPatterns: RegExp[] = [
		/(?:^|\n|\s)(?:Praxisinhaber(?:in)?|Inhaber(?:in)?(?:\s+der\s+Praxis)?)\s*[.:]?\s*\n?\s*([^\n]+(?:\n\s*[^\n]+){0,2})/gi,
		/(?:^|\n|\s)(?:Vertreten\s+durch|Vertretungsberechtigt(?:er|e)?)\s*[.:]?\s*\n?\s*([^\n]+(?:\n\s*[^\n]+){0,2})/gi,
		/(?:^|\n|\s)(?:Gesch[äa]ftsf[üu]hrer(?:in)?|Gesch[äa]ftsleitung)\s*[.:]?\s*\n?\s*([^\n]+(?:\n\s*[^\n]+){0,2})/gi,
		/(?:^|\n|\s)(?:Inhaltlich\s+Verantwortlich(?:er)?(?:\s+(?:gem(?:äß)?|nach)[^\n:]*)?)\s*[.:]?\s*\n?\s*([^\n]+(?:\n\s*[^\n]+){0,2})/gi,
		/(?:^|\n|\s)(?:Verantwortlich\s+(?:für\s+den\s+Inhalt|i\.?\s*S\.?\s*d\.?\s*§|im\s+Sinne|gemäß|nach\s+§|gem\.?\s+§)[^\n:]*)\s*[.:]?\s*\n?\s*([^\n]+(?:\n\s*[^\n]+){0,2})/gi,
		/(?:^|\n|\s)(?:(?:Praxis)?Leitung|Praxisleiter(?:in)?|Zahnärztlicher\s+Leiter(?:in)?)\s*[.:]?\s*\n?\s*([^\n]+(?:\n\s*[^\n]+){0,2})/gi,
		/(?:^|\n|\s)(?:Betreiber(?:in)?(?:\s+(?:der\s+(?:Website|Webseite|Seite|Internetpräsenz)|dieser\s+(?:Website|Seite|Webseite|Internetpräsenz)|dieses\s+(?:Onlineangebotes|Internetauftrittes?)))?)\s*[.:]?\s*\n?\s*([^\n]+(?:\n\s*[^\n]+){0,2})/gi,
	];

	const candidates: string[] = [];
	for (const pattern of labelPatterns) {
		for (const m of businessText.matchAll(pattern)) {
			const block = m[1]?.trim();
			if (!block) continue;
			// Split the 1–3 captured lines into individual candidates so each is
			// tried separately. The first line often holds the company name; the
			// person is on line 2 or 3.
			const lines = block.split(/\n/).map((l) => l.trim()).filter((l) => l.length >= 3 && l.length <= 200);
			for (const v of lines) {
				if (/^§|^(?:Die|Der|Das|Ein|Eine)\s|^und\s|^sowie\s/i.test(v)) continue;
				if (/\b(?:ist|sind|werden|haben|kann|können|sollen|bitte|unsere[nmrs]?|diese[nmrs]?)\b/i.test(v)) continue;
				candidates.push(v);
			}
		}
	}

	// Fallback: postal-code anchor. The 3 lines preceding "12345 Stadt" often hold the person.
	if (candidates.length === 0) {
		const lines = businessText.split('\n').map((l) => l.trim()).filter((l) => l.length > 0);
		for (let i = 0; i < lines.length; i++) {
			if (/^\d{5}\s+[A-ZÄÖÜ]/.test(lines[i])) {
				for (let j = Math.max(0, i - 3); j < i; j++) {
					if (looksLikePersonName(lines[j])) candidates.push(lines[j]);
				}
				break;
			}
		}
	}

	// Two-pass: first prefer candidates that yield BOTH firstName and lastName,
	// then fall back to candidates with just a lastName.
	for (const candidate of candidates) {
		const parsed = parsePersonFromLabelValue(candidate);
		if (!parsed) continue;
		if (isRejectedPerson(parsed.firstName, parsed.lastName)) continue;
		if (parsed.firstName && parsed.lastName) return parsed;
	}
	for (const candidate of candidates) {
		const parsed = parsePersonFromLabelValue(candidate);
		if (!parsed) continue;
		if (isRejectedPerson(parsed.firstName, parsed.lastName)) continue;
		if (parsed.firstName || parsed.lastName) return parsed;
	}

	return empty;
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

const UNIVERSAL_EMAIL_PREFIXES = [
	'info', 'kontakt', 'contact', 'office', 'praxis', 'mail',
	'post', 'empfang', 'rezeption', 'verwaltung', 'service',
	'hello', 'hallo', 'anfrage', 'zentrale',
];

/**
 * Sorts emails so the decision-maker's address comes first, followed by
 * universal/generic addresses (info@, kontakt@, etc.), then everything else.
 */
function prioritizeEmails(
	emails: string[],
	firstName: string | null,
	lastName: string | null,
): string[] {
	if (emails.length <= 1) return emails;

	const nameTokens: string[] = [];
	if (firstName) nameTokens.push(firstName.toLowerCase());
	if (lastName) nameTokens.push(lastName.toLowerCase());

	function score(email: string): number {
		const local = email.toLowerCase().split('@')[0] || '';

		// Score 0 (highest priority): email contains the person's name parts
		if (nameTokens.length > 0 && nameTokens.some((t) => t.length >= 2 && local.includes(t))) {
			return 0;
		}

		// Score 2 (lowest): universal/generic prefixes
		if (UNIVERSAL_EMAIL_PREFIXES.some((p) => local === p)) {
			return 2;
		}

		// Score 1: other personal-looking emails (not generic, not matched by name)
		return 1;
	}

	return [...emails].sort((a, b) => score(a) - score(b));
}

function normalizeEmail(email: string): string {
	let result = email
		.replace(/\s*\(at\)\s*/gi, '@')
		.replace(/\s*\[at\]\s*/gi, '@')
		.replace(/\s*\(a\)\s*/gi, '@')
		.replace(/\s*\{at\}\s*/gi, '@')
		.replace(/&#64;/g, '@')
		.replace(/&#46;/g, '.')
		.replace(/\s*\(dot\)\s*/gi, '.')
		.replace(/\s*\[dot\]\s*/gi, '.')
		.replace(/%40/g, '@');
	// Only apply bare-word "at" → "@" when no @ is present yet,
	// to avoid corrupting valid .at domains (e.g. ooeg.at)
	if (!result.includes('@')) {
		result = result.replace(/\s+at\s+/gi, '@');
	}
	return result.trim();
}

/** Basic sanity check that a string looks like a real email address. */
function isValidEmail(email: string): boolean {
	// Must contain exactly one @
	const parts = email.split('@');
	if (parts.length !== 2) return false;
	const [local, domain] = parts;
	if (!local || !domain) return false;
	// Reject URL-encoding leftovers (%20, %40, etc.)
	if (/%[0-9A-Fa-f]{2}/.test(email)) return false;
	// Reject retina image filenames like image@2x.png, icon@4x.webp
	if (/^.+@\d+x\.\w+$/.test(email)) return false;
	// Local part: only word chars, dots, hyphens, plus signs
	if (!/^[\w.+\-]+$/.test(local)) return false;
	// Domain: must have at least one dot, only word chars, dots, hyphens
	if (!/^[\w.-]+\.\w{2,}$/.test(domain)) return false;
	return true;
}

/** Clean up a raw email extracted from a mailto: href. */
function cleanMailtoEmail(raw: string): string {
	let email = raw;
	// Decode common URL-encoded characters
	try { email = decodeURIComponent(email); } catch { /* keep as-is */ }
	// Strip HTML entities
	email = email.replace(/&#64;/g, '@').replace(/&#46;/g, '.');
	return email.trim();
}

function extractEmails(html: string, businessText: string, siteDomain?: string): string[] {
	const found: string[] = [];
	const seen = new Set<string>();

	/** Try to deobfuscate a placeholder email by replacing its domain with the site domain. */
	const deobfuscate = (email: string): string => {
		if (!siteDomain) return email;
		const lower = email.toLowerCase().trim();
		if (isPlaceholderEmail(lower)) {
			const localPart = lower.split('@')[0];
			return `${localPart}@${siteDomain}`;
		}
		return email;
	};

	const addEmail = (email: string) => {
		const resolved = deobfuscate(email);
		const normalized = resolved.toLowerCase().trim();
		if (
			!seen.has(normalized) &&
			!isChamberEmail(normalized) &&
			!isPlaceholderEmail(normalized) &&
			isValidEmail(normalized)
		) {
			seen.add(normalized);
			found.push(resolved.trim());
		}
	};

	const htmlLower = html.toLowerCase();
	let impressumStart = htmlLower.indexOf('impressum');
	if (impressumStart === -1) impressumStart = 0;
	const impressumHtml = html.substring(impressumStart);

	// mailto links in impressum section
	const mailtoRegex = /mailto:([^\s"'<>?]+)/gi;
	let match;
	while ((match = mailtoRegex.exec(impressumHtml)) !== null) {
		addEmail(cleanMailtoEmail(match[1]));
	}

	// Obfuscated patterns
	const obfuscatedPatterns = [
		/[\w.-]+\s*\(a\)\s*[\w.-]+\.\w{2,}/gi,
		/[\w.-]+\s*\[at\]\s*[\w.-]+\.\w{2,}/gi,
		/[\w.-]+\s*\(at\)\s*[\w.-]+\.\w{2,}/gi,
	];
	for (const p of obfuscatedPatterns) {
		while ((match = p.exec(businessText)) !== null) {
			const email = match[0]
				.replace(/\s*\(a\)\s*/gi, '@')
				.replace(/\s*\[at\]\s*/gi, '@')
				.replace(/\s*\(at\)\s*/gi, '@');
			addEmail(email);
		}
	}

	// Standard email regex
	const emailRegex = /[\w.-]+@[\w.-]+\.\w{2,}/g;
	while ((match = emailRegex.exec(businessText)) !== null) {
		addEmail(match[0]);
	}

	// Fallback: all mailto in full HTML
	if (found.length === 0) {
		const fallbackRegex = /mailto:([^\s"'<>?]+)/gi;
		while ((match = fallbackRegex.exec(html)) !== null) {
			addEmail(cleanMailtoEmail(match[1]));
		}
	}

	return found;
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

function isPlaceholderEmail(email: string): boolean {
	const domain = email.split('@')[1] || '';
	const placeholderDomains = [
		'example.com', 'example.org', 'example.net',
		'domain.com', 'domain.de', 'domain.tld',
		'domain1.tld', 'domain2.tld',
		'muster.de', 'musterfirma.de',
		'test.com', 'test.de', 'test.tld',
		'localhost', 'email.com', 'email.de',
		'ihre-domain.de', 'your-domain.com',
		'platzhalter.de', 'placeholder.com',
	];
	if (placeholderDomains.includes(domain)) return true;
	// Catch patterns like domain<N>.tld, example<N>.com
	if (/^(domain|example|test|muster)\d*\.\w+$/.test(domain)) return true;
	return false;
}

/** Check if HTML contains email addresses with placeholder domains (JS obfuscation pattern). */
function htmlHasPlaceholderEmails(html: string): boolean {
	const emailRegex = /[\w.-]+@[\w.-]+\.\w{2,}/g;
	let match;
	while ((match = emailRegex.exec(html)) !== null) {
		if (isPlaceholderEmail(match[0].toLowerCase())) return true;
	}
	return false;
}

function extractPhones(businessText: string): string[] {
	const found: string[] = [];
	const patterns = [
		/(?:Tel(?:efon)?|Phone|Fon)\s*[.:]+\s*([+\d][\d\s/\-\u2013\u2014().]+\d)/gi,
		/(?:Tel(?:efon)?|Phone|Fon)\s+([+\d][\d\s/\-\u2013\u2014().]+\d)/gi,
		/T\s*[.:]\s*([+\d][\d\s/\-\u2013\u2014().]+\d)/g,
	];
	for (const p of patterns) {
		let m;
		while ((m = p.exec(businessText)) !== null) {
			const raw = m[1].trim();
			if (!found.includes(raw)) found.push(raw);
		}
	}
	return found;
}

function extractFaxNumbers(businessText: string): string[] {
	const found: string[] = [];
	const patterns = [
		/(?:Fax|Telefax)\s*[.:]+\s*([+\d][\d\s/\-\u2013\u2014().]+\d)/gi,
		/(?:Fax|Telefax)\s+([+\d][\d\s/\-\u2013\u2014().]+\d)/gi,
	];
	for (const p of patterns) {
		let m;
		while ((m = p.exec(businessText)) !== null) {
			const raw = m[1].trim();
			if (!found.includes(raw)) found.push(raw);
		}
	}
	return found;
}

function extractMobileNumbers(businessText: string): string[] {
	const found: string[] = [];
	const regex = /(?:Mobil|Handy|Mobile)\s*[.:]\s*([+\d][\d\s/\-\u2013\u2014().]+\d)/gi;
	let m;
	while ((m = regex.exec(businessText)) !== null) {
		const raw = m[1].trim();
		if (!found.includes(raw)) found.push(raw);
	}
	return found;
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

// ═══════════════════════════════════════════════════════════════════════════════
// Domain Guessing (deterministic + OpenAI fallback)
// ═══════════════════════════════════════════════════════════════════════════════

const DOMAIN_UMLAUT_MAP: Record<string, string> = { 'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss' };

function transliterate(s: string): string {
	return s.toLowerCase().replace(/[äöüß]/g, (c) => DOMAIN_UMLAUT_MAP[c] || c);
}

function slugify(s: string): string {
	return transliterate(s)
		.replace(/[^a-z0-9]+/g, '-')
		.replace(/^-+|-+$/g, '');
}

const TITLE_RE = /^(dr\.?\s*(med\.?\s*(dent\.?)?)?|prof\.?\s*(dr\.?\s*(med\.?\s*(dent\.?)?)?)?|dipl\.\s*\w+\.?)\s*/i;
const LEGAL_SUFFIX_RE = /\s+(gmbh|gbr|ohg|ug|ag|e\.?\s*k\.?|partg\s*mbb|mbh)\.?$/i;
const PROFESSION_RE = /^(zahnarzt(?:praxis)?|praxis|kieferorthop(?:ae|[aä])d(?:ie|ische?\s+praxis)?|arzt(?:praxis)?|klinik|zentrum|institut)\s+/i;

/**
 * Deterministically guesses likely domain names for a German company.
 * Covers the common patterns without needing an AI call.
 */
function guessDomainsLocal(companyName: string, city: string): string[] {
	const guesses = new Set<string>();
	const cleaned = companyName.trim();

	// Strip title for a "no-title" variant
	const noTitle = cleaned.replace(TITLE_RE, '').trim();
	// Strip legal suffix
	const noSuffix = noTitle.replace(LEGAL_SUFFIX_RE, '').trim();

	// Full slug (e.g. "zahnarztpraxis-dr-mueller" or "lange-und-rakhimov")
	const fullSlug = slugify(noSuffix);
	if (fullSlug.length >= 3) guesses.add(`${fullSlug}.de`);

	// Without title (e.g. "zahnarztpraxis-mueller")
	const noTitleSlug = slugify(noSuffix.replace(TITLE_RE, '').trim());
	if (noTitleSlug.length >= 3 && noTitleSlug !== fullSlug) guesses.add(`${noTitleSlug}.de`);

	// With city (e.g. "zahnarzt-mueller-dresden")
	const citySlug = city ? slugify(city) : '';
	if (citySlug && fullSlug.length >= 3) guesses.add(`${fullSlug}-${citySlug}.de`);
	if (citySlug && noTitleSlug.length >= 3 && noTitleSlug !== fullSlug) guesses.add(`${noTitleSlug}-${citySlug}.de`);

	// Extract last name from patterns like "Zahnarztpraxis Dr. Müller" or "Praxis Müller"
	const profMatch = noSuffix.match(PROFESSION_RE);
	if (profMatch) {
		const rest = noSuffix.slice(profMatch[0].length).replace(TITLE_RE, '').trim();
		const parts = rest.split(/\s+/);
		const lastName = parts[parts.length - 1];
		if (lastName && lastName.length >= 2) {
			const lastSlug = slugify(lastName);
			const profSlug = slugify(profMatch[1]);
			guesses.add(`${profSlug}-${lastSlug}.de`);
			guesses.add(`praxis-${lastSlug}.de`);
			if (citySlug) guesses.add(`${profSlug}-${lastSlug}-${citySlug}.de`);
		}
	}

	return [...guesses].slice(0, 8);
}

/**
 * AI fallback: asks OpenAI to guess likely domain names for a German company.
 * Only called when deterministic guesses all failed verification.
 */
async function guessDomainsAi(
	ctx: IExecuteFunctions,
	companyName: string,
	city: string,
	openAiKey: string,
	model: string,
): Promise<string[]> {
	try {
		const response = await ctx.helpers.httpRequest({
			method: 'POST',
			url: 'https://api.openai.com/v1/chat/completions',
			headers: {
				Authorization: `Bearer ${openAiKey}`,
				'Content-Type': 'application/json',
			},
			body: {
				model,
				temperature: 0.3,
				messages: [
					{
						role: 'system',
						content:
							'You are a German business website domain guesser. Given a company name' + (city ? ' and city' : '') + ', guess the most likely website domains. German businesses typically use patterns like:\n' +
							'- company-name.de (e.g. "lange-und-rakhimov.de")\n' +
							(city ? '- company-name-city.de (e.g. "zahnarzt-roeder-dresden.de")\n' : '') +
							'- zahnarztpraxis-lastname.de\n' +
							'- zahnarzt-lastname.de\n' +
							'- praxis-lastname.de\n\n' +
							'Rules:\n' +
							'- Umlauts are transliterated: ä→ae, ö→oe, ü→ue, ß→ss\n' +
							'- Spaces and special chars become hyphens\n' +
							'- Drop academic titles like "Dr." or "Prof." OR keep them (try both)\n' +
							'- Try .de domain\n' +
							'- Return ONLY a JSON array of up to 8 domain guesses, most likely first\n' +
							'- No explanations, just the JSON array',
					},
					{
						role: 'user',
						content: city ? `Company: ${companyName}\nCity: ${city}` : `Company: ${companyName}`,
					},
				],
			},
			timeout: 15000,
		});

		const content = response?.choices?.[0]?.message?.content?.trim();
		if (!content) return [];

		const jsonMatch = content.match(/\[[\s\S]*\]/);
		if (jsonMatch) {
			const parsed = JSON.parse(jsonMatch[0]);
			if (Array.isArray(parsed)) {
				return parsed.filter((d: unknown) => typeof d === 'string' && d.length > 0);
			}
		}
		return [];
	} catch {
		return [];
	}
}

/**
 * Checks if a domain exists by sending a HEAD request.
 * Returns true if the server responds (any 2xx/3xx status).
 */
async function domainExists(
	ctx: IExecuteFunctions,
	domain: string,
	timeout: number,
): Promise<boolean> {
	try {
		const response = await ctx.helpers.httpRequest({
			method: 'HEAD',
			url: `https://${domain}`,
			headers: {
				'User-Agent': USER_AGENT,
			},
			returnFullResponse: true,
			timeout: Math.min(timeout, 8000),
			ignoreHttpStatusErrors: true,
		});

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const fullResp = response as any;
		return fullResp.statusCode >= 200 && fullResp.statusCode < 400;
	} catch {
		// Try HTTP as fallback
		try {
			const response = await ctx.helpers.httpRequest({
				method: 'HEAD',
				url: `http://${domain}`,
				headers: {
					'User-Agent': USER_AGENT,
				},
				returnFullResponse: true,
				timeout: Math.min(timeout, 8000),
				ignoreHttpStatusErrors: true,
			});

			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const fullResp = response as any;
			return fullResp.statusCode >= 200 && fullResp.statusCode < 400;
		} catch {
			return false;
		}
	}
}
