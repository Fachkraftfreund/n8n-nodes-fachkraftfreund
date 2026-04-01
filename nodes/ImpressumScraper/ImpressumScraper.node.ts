import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
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
			'Searches for a company by name and city, finds the homepage via Google, then crawls the website to extract structured Impressum/legal data.',
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
				required: true,
				description: 'The city where the company is located',
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
			const city = this.getNodeParameter('city', i) as string;
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
					batch[j].error = `Search failed: ${(settled[j] as PromiseRejectedResult).reason?.message || 'Unknown error'}`;
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
				await Promise.allSettled(
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
				job.error = `No search results found for "${job.companyName}" in "${job.city}"`;
				continue;
			}

			// Items with >1 result need OpenAI to pick the homepage
			if (openAiKey && gr.filtered.length > 1) {
				aiPickJobs.push({ job, query, filtered: gr.filtered });
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
					job.error = `No homepage found for "${job.companyName}" in "${job.city}" (checked ${dirUrls.length} directory pages)`;
				}
			}
		}

		// ── Phase 0c: Domain guessing fallback (OpenAI) ─────────────
		if (openAiKey) {
			const jobsStillNeedingHomepage = jobs.filter(
				(j) => !j.normalizedUrl && !j.error,
			);
			// Also include jobs that got an error in Phase 0b (directory failure)
			const jobsFromDirFailure = jobs.filter(
				(j) => j.error && j.error.includes('checked') && j.error.includes('directory pages'),
			);
			const allGuessJobs = [...jobsStillNeedingHomepage, ...jobsFromDirFailure];

			if (allGuessJobs.length > 0) {
				for (let i = 0; i < allGuessJobs.length; i += OPENAI_CONCURRENCY) {
					const batch = allGuessJobs.slice(i, i + OPENAI_CONCURRENCY);
					const settled = await Promise.allSettled(
						batch.map(async (job) => {
							const guesses = await guessDomains(this, job.companyName, job.city, openAiKey, openAiModel);
							if (guesses.length === 0) return;

							// Verify which domains exist (parallel HEAD requests)
							const checks = await Promise.allSettled(
								guesses.slice(0, 8).map(async (domain) => {
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
									return;
								}
							}
						}),
					);
					// Errors are silently ignored — domain guessing is best-effort
					void settled;
				}
			}
		}

		// Mark remaining jobs with no homepage and no error
		for (const job of jobs) {
			if (!job.error && !job.normalizedUrl) {
				job.error = `No homepage found for "${job.companyName}" in "${job.city}"`;
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

		// ── Phase 6: Plausibility check + OpenAI enrichment ─────────
		if (openAiKey && successfulJobs.length > 0) {
			await enrichWithOpenAi(this, successfulJobs, openAiKey, openAiModel);
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
	} catch {
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

	const keywords = [...normalize(companyName).split(/\s+/), ...normalize(city).split(/\s+/)]
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

const EXTRACTABLE_FIELDS: Record<string, string> = {
	companyName: 'Company or practice name (Firmenname / Praxisname)',
	salutation: 'Salutation: "Herr" or "Frau" only',
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

function sanitizeString(value: string): string {
	return value
		.replace(/\x00/g, '')                // null bytes
		.replace(/\\u[0-9a-fA-F]{4}/g, '')   // unicode escape sequences
		.replace(/\\/g, '');                  // stray backslashes
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
// Salutation Derivation from First Name
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Batches all firstNames missing a salutation into a single OpenAI call.
 * Returns "Herr" or "Frau" for each name.
 */
async function deriveSalutations(
	ctx: IExecuteFunctions,
	results: Array<{ job: ScrapeJob; data: ImpressumResult; text: string }>,
	openAiKey: string,
	model: string,
): Promise<void> {
	const needsSalutation: Array<{ idx: number; firstName: string }> = [];

	for (let i = 0; i < results.length; i++) {
		const { data } = results[i];
		if (!data.salutation && data.firstName) {
			needsSalutation.push({ idx: i, firstName: data.firstName });
		}
	}

	if (needsSalutation.length === 0) return;

	const nameList = needsSalutation.map((n) => n.firstName).join(', ');

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

		for (const { idx, firstName } of needsSalutation) {
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
// Phone Number Normalization via OpenAI
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Sends all raw phone/fax/mobile strings to OpenAI for normalization.
 * Handles weird formats like "(0124) 23123 43 / 44" → two separate numbers.
 */
async function normalizePhoneNumbers(
	ctx: IExecuteFunctions,
	results: Array<{ job: ScrapeJob; data: ImpressumResult; text: string }>,
	openAiKey: string,
	model: string,
): Promise<void> {
	const rawNumbers: Array<{ resultIdx: number; field: 'phones' | 'faxNumbers' | 'mobileNumbers'; raw: string }> = [];

	for (let i = 0; i < results.length; i++) {
		const { data } = results[i];
		for (const num of data.phones) rawNumbers.push({ resultIdx: i, field: 'phones', raw: num });
		for (const num of data.faxNumbers) rawNumbers.push({ resultIdx: i, field: 'faxNumbers', raw: num });
		for (const num of data.mobileNumbers) rawNumbers.push({ resultIdx: i, field: 'mobileNumbers', raw: num });
	}

	if (rawNumbers.length === 0) return;

	const numberList = rawNumbers.map((n, i) => `${i}: ${n.raw}`).join('\n');

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
		if (!content) return;

		const mapping: Record<string, string[]> = JSON.parse(content);

		// Clear and rebuild arrays per result
		for (const result of results) {
			result.data.phones = [];
			result.data.faxNumbers = [];
			result.data.mobileNumbers = [];
		}

		for (let i = 0; i < rawNumbers.length; i++) {
			const { resultIdx, field } = rawNumbers[i];
			const normalized = mapping[String(i)];
			if (Array.isArray(normalized)) {
				results[resultIdx].data[field].push(
					...normalized.filter((n) => typeof n === 'string' && n.length > 0),
				);
			} else {
				// Fallback: keep original
				results[resultIdx].data[field].push(rawNumbers[i].raw);
			}
		}
	} catch {
		// Phone normalization is best-effort
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
// Domain Guessing (OpenAI + verification)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Asks OpenAI to guess likely domain names for a German company.
 * Returns an array of domain guesses (e.g. ["zahnarzt-mueller.de", "praxis-mueller-berlin.de"]).
 */
async function guessDomains(
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
							'You are a German business website domain guesser. Given a company name and city, guess the most likely website domains. German businesses typically use patterns like:\n' +
							'- company-name.de (e.g. "lange-und-rakhimov.de")\n' +
							'- company-name-city.de (e.g. "zahnarzt-roeder-dresden.de")\n' +
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
						content: `Company: ${companyName}\nCity: ${city}`,
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
