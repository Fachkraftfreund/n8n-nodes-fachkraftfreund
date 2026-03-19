import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';

export class ImpressumScraper implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Impressum Scraper',
		name: 'impressumScraper',
		icon: 'file:impressum.svg',
		group: ['transform'],
		version: 1,
		subtitle: 'Scrape Impressum data from websites',
		description: 'Crawls a website to find its Impressum page and extracts structured contact/legal data (name, email, phone, fax, address, tax ID, etc.)',
		defaults: {
			name: 'Impressum Scraper',
		},
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
				name: 'apifyApi',
				required: false,
				displayOptions: {
					show: {
						scrapingMode: ['apifyCheerio', 'apifyWeb'],
					},
				},
			},
		],
		properties: [
			{
				displayName: 'URL',
				name: 'url',
				type: 'string',
				default: '',
				required: true,
				description: 'The website URL to scrape for Impressum data. Can be a homepage URL — the node will automatically find the Impressum page.',
				placeholder: 'https://example.de',
			},
			{
				displayName: 'Scraping Mode',
				name: 'scrapingMode',
				type: 'options',
				options: [
					{
						name: 'Direct HTTP',
						value: 'directHttp',
						description: 'Fast direct HTTP requests. Works for most static sites.',
					},
					{
						name: 'Apify Cheerio Scraper',
						value: 'apifyCheerio',
						description: 'Uses Apify Cheerio Scraper. Good for sites that block direct requests.',
					},
					{
						name: 'Apify Web Scraper',
						value: 'apifyWeb',
						description: 'Uses Apify Web Scraper with browser rendering. For JavaScript-heavy sites.',
					},
				],
				default: 'directHttp',
				description: 'How to fetch the web pages',
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
						description: 'Timeout for HTTP requests in seconds',
					},
					{
						displayName: 'Try Common Paths',
						name: 'tryCommonPaths',
						type: 'boolean',
						default: true,
						description: 'Whether to try common Impressum URL paths (/impressum, /imprint, etc.) if no link is found in the HTML',
					},
					{
						displayName: 'Check Homepage for Impressum',
						name: 'checkHomepage',
						type: 'boolean',
						default: true,
						description: 'Whether to check if the homepage itself contains Impressum content',
					},
				],
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		for (let i = 0; i < items.length; i++) {
			try {
				const url = this.getNodeParameter('url', i) as string;
				const scrapingMode = this.getNodeParameter('scrapingMode', i) as string;
				const options = this.getNodeParameter('options', i) as {
					timeout?: number;
					tryCommonPaths?: boolean;
					checkHomepage?: boolean;
				};

				const timeout = (options.timeout ?? 15) * 1000;
				const tryCommonPaths = options.tryCommonPaths !== false;
				const checkHomepage = options.checkHomepage !== false;

				// Validate URL
				let normalizedUrl = url.trim();
				if (!normalizedUrl.match(/^https?:\/\//i)) {
					normalizedUrl = 'https://' + normalizedUrl;
				}
				try {
					new URL(normalizedUrl);
				} catch {
					throw new NodeOperationError(this.getNode(), `Invalid URL: ${url}`, { itemIndex: i });
				}

				// Get Apify token if needed
				let apifyToken: string | undefined;
				if (scrapingMode === 'apifyCheerio' || scrapingMode === 'apifyWeb') {
					const credentials = await this.getCredentials('apifyApi');
					apifyToken = credentials.apiToken as string;
				}

				// Step 1: Fetch homepage
				let homepageHtml: string;
				let finalUrl: string;
				try {
					const result = await fetchPage(this, normalizedUrl, scrapingMode, apifyToken, timeout);
					homepageHtml = result.html;
					finalUrl = result.finalUrl;
				} catch (err) {
					// If direct HTTP fails, try with Apify as fallback
					if (scrapingMode === 'directHttp' && apifyToken) {
						const result = await fetchPage(this, normalizedUrl, 'apifyCheerio', apifyToken, timeout);
						homepageHtml = result.html;
						finalUrl = result.finalUrl;
					} else {
						throw err;
					}
				}

				// Step 2: Find impressum URL
				let impressumUrl = findImpressumUrl(homepageHtml, finalUrl);

				if (!impressumUrl && tryCommonPaths) {
					impressumUrl = await tryCommonImpressumPaths(this, finalUrl, scrapingMode, apifyToken, timeout);
				}

				if (!impressumUrl && checkHomepage) {
					const homepageText = htmlToText(homepageHtml);
					if (looksLikeImpressum(homepageText)) {
						impressumUrl = finalUrl;
					}
				}

				if (!impressumUrl) {
					if (this.continueOnFail()) {
						returnData.push({
							json: {
								sourceUrl: normalizedUrl,
								error: 'No Impressum page found',
								success: false,
							},
						});
						continue;
					}
					throw new NodeOperationError(
						this.getNode(),
						`No Impressum page found for ${normalizedUrl}`,
						{ itemIndex: i },
					);
				}

				// Step 3: Fetch impressum page
				let impressumHtml: string;
				if (impressumUrl === finalUrl) {
					impressumHtml = homepageHtml;
				} else {
					const result = await fetchPage(this, impressumUrl, scrapingMode, apifyToken, timeout);
					impressumHtml = result.html;
				}

				// Step 4: Parse impressum
				const text = htmlToText(impressumHtml);
				const data = extractImpressumData(impressumHtml, text, impressumUrl, normalizedUrl);

				returnData.push({ json: { ...data, success: true } });

			} catch (error) {
				if (this.continueOnFail()) {
					const url = this.getNodeParameter('url', i, '') as string;
					returnData.push({
						json: {
							sourceUrl: url,
							error: (error as Error).message,
							success: false,
						},
					});
					continue;
				}
				throw error;
			}
		}

		return [returnData];
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Page Fetching
// ═══════════════════════════════════════════════════════════════════════════════

async function fetchPage(
	ctx: IExecuteFunctions,
	url: string,
	mode: string,
	apifyToken: string | undefined,
	timeout: number,
): Promise<{ html: string; finalUrl: string }> {
	if (mode === 'directHttp') {
		return fetchDirect(ctx, url, timeout);
	} else {
		return fetchViaApify(ctx, url, mode, apifyToken!, timeout);
	}
}

async function fetchDirect(
	ctx: IExecuteFunctions,
	url: string,
	timeout: number,
): Promise<{ html: string; finalUrl: string }> {
	const response = await ctx.helpers.httpRequest({
		method: 'GET',
		url,
		headers: {
			'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
			'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
			'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
		},
		returnFullResponse: true,
		timeout,
	});

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const fullResp = response as any;
	const html = typeof fullResp.body === 'string' ? fullResp.body : JSON.stringify(fullResp.body);
	return { html, finalUrl: url };
}

async function fetchViaApify(
	ctx: IExecuteFunctions,
	url: string,
	mode: string,
	apifyToken: string,
	_timeout: number,
): Promise<{ html: string; finalUrl: string }> {
	const actorId = mode === 'apifyCheerio' ? 'apify~cheerio-scraper' : 'apify~web-scraper';

	const pageFunction = `async function pageFunction(context) {
		const { ${mode === 'apifyCheerio' ? '$' : 'jQuery: $'}, request } = context;
		return {
			url: request.loadedUrl || request.url,
			html: ${mode === 'apifyCheerio' ? '$.html()' : '$("html").html()'},
			title: $('title').text(),
		};
	}`;

	const response = await ctx.helpers.httpRequest({
		method: 'POST',
		url: `https://api.apify.com/v2/acts/${actorId}/run-sync-get-dataset-items`,
		qs: { token: apifyToken },
		headers: { 'Content-Type': 'application/json' },
		body: {
			startUrls: [{ url }],
			maxRequestsPerCrawl: 1,
			pageFunction,
		},
		timeout: 120000, // Apify actor runs can take a while
	});

	const items = (Array.isArray(response) ? response : []) as Array<{ url: string; html: string }>;
	if (items.length === 0) {
		throw new Error(`Apify scraper returned no results for ${url}`);
	}
	return { html: items[0].html, finalUrl: items[0].url };
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
			} catch { /* skip invalid */ }
		}
	}

	candidates.sort((a, b) => b.score - a.score);
	return candidates.length > 0 ? candidates[0].url : null;
}

async function tryCommonImpressumPaths(
	ctx: IExecuteFunctions,
	baseUrl: string,
	mode: string,
	apifyToken: string | undefined,
	timeout: number,
): Promise<string | null> {
	const base = new URL(baseUrl);
	const paths = [
		'/impressum',
		'/impressum/',
		'/impressum.html',
		'/impressum.php',
		'/imprint',
		'/imprint/',
	];

	for (const path of paths) {
		const testUrl = new URL(path, base).href;
		try {
			if (mode === 'directHttp') {
				const response = await ctx.helpers.httpRequest({
					method: 'GET',
					url: testUrl,
					headers: {
						'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
					},
					returnFullResponse: true,
					timeout,
					ignoreHttpStatusErrors: true,
				});
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
				const fullResp = response as any;
				if (fullResp.statusCode >= 200 && fullResp.statusCode < 400) {
					const body = typeof fullResp.body === 'string' ? fullResp.body : '';
					if (body.length > 500 && looksLikeImpressum(htmlToText(body))) {
						return testUrl;
					}
				}
			} else {
				// For Apify modes, just try fetching
				const result = await fetchViaApify(ctx, testUrl, mode, apifyToken!, timeout);
				if (result.html.length > 500 && looksLikeImpressum(htmlToText(result.html))) {
					return testUrl;
				}
			}
		} catch {
			// Skip this path
		}
	}
	return null;
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
	// Also check for typical impressum content
	if (lower.includes('impressum') && (lower.includes('telefon') || lower.includes('tel.'))) score++;
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
	text = text.replace(/<\/(?:p|div|h[1-6]|li|tr|section|article|header|footer|main|aside|nav)>/gi, '\n');
	text = text.replace(/<br\s*\/?>/gi, '\n');
	text = text.replace(/<\/(?:td|th)>/gi, ' ');
	text = text.replace(/<[^>]+>/g, ' ');
	const entities: Record<string, string> = {
		'&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', '&apos;': "'",
		'&nbsp;': ' ', '&ouml;': 'ö', '&auml;': 'ä', '&uuml;': 'ü',
		'&Ouml;': 'Ö', '&Auml;': 'Ä', '&Uuml;': 'Ü', '&szlig;': 'ß',
	};
	for (const [ent, char] of Object.entries(entities)) {
		text = text.split(ent).join(char);
	}
	text = text.replace(/&#(\d+);/g, (_, code) => String.fromCharCode(parseInt(code)));
	text = text.replace(/&#x([0-9a-fA-F]+);/g, (_, code) => String.fromCharCode(parseInt(code, 16)));
	text = text.replace(/[^\S\n]+/g, ' ');
	text = text.split('\n').map(l => l.trim()).join('\n');
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

function extractImpressumData(html: string, fullText: string, impressumUrl: string, sourceUrl: string): ImpressumResult {
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

function splitBusinessAndRegulatory(section: string): { business: string; regulatory: string } {
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
	const lines = businessText.split('\n').map(l => l.trim()).filter(l => l.length > 0);

	// Known non-company-name patterns
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
			if (p.test(line)) { skip = true; break; }
		}
		if (skip) continue;

		// A company name typically contains words and maybe legal form indicators
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

	// Patterns to find the responsible person
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
			// If the match is a section header or too long, skip it
			if (nameString.length > 80 || /^§|^(?:Die|Der|Das|Ein|Eine)\s/i.test(nameString)) {
				nameString = null;
				continue;
			}
			break;
		}
	}

	// If no role label found, look for a person name near the address
	if (!nameString) {
		const lines = businessText.split('\n').map(l => l.trim()).filter(l => l.length > 0);
		for (let i = 0; i < lines.length; i++) {
			if (/^\d{5}\s+[A-ZÄÖÜ]/.test(lines[i])) {
				// Look above the PLZ line for a person name
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

	// Handle multiple people: take only the first one
	nameString = nameString.split(/\s*(?:&|(?:\s+und\s+))\s*/)[0].trim();
	nameString = nameString.split(/\s*,\s*/)[0].trim();

	// Remove parenthetical and dash-separated suffixes
	nameString = nameString.replace(/\s*\(.*\)/, '');
	nameString = nameString.replace(/\s*[-–].*$/, '');

	// Extract salutation
	const salutationMatch = nameString.match(/^(Herr(?:n)?|Frau)\s+/i);
	if (salutationMatch) {
		result.salutation = salutationMatch[1].replace(/^Herrn$/i, 'Herr');
		nameString = nameString.substring(salutationMatch[0].length).trim();
	}

	// Extract academic/professional titles
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

	// Split remaining into first name + last name
	const nameParts = nameString.split(/\s+/).filter(p => p.length > 0);

	// Validate: parts should look like actual names
	const validParts = nameParts.filter(p =>
		/^[A-ZÄÖÜ][a-zäöüß]+$/.test(p) ||
		/^[A-ZÄÖÜ]\.$/.test(p) ||
		/^[A-ZÄÖÜ][a-zäöüß]+-[A-ZÄÖÜ][a-zäöüß]+$/.test(p) // Hyphenated names
	);

	if (validParts.length >= 2) {
		result.firstName = validParts[0];
		result.lastName = validParts.slice(1).join(' ');
	} else if (nameParts.length >= 2 && nameParts.every(p => /^[A-ZÄÖÜ]/.test(p))) {
		result.firstName = nameParts[0];
		result.lastName = nameParts.slice(1).join(' ');
	} else if (nameParts.length === 1 && /^[A-ZÄÖÜ]/.test(nameParts[0])) {
		result.lastName = nameParts[0];
	}

	return result;
}

function looksLikePersonName(line: string): boolean {
	if (line.length < 3 || line.length > 80) return false;
	if (/^(Tel|Fax|E-?Mail|Telefon|Telefax|www\.|http|Impressum|Angaben|Kontakt|Vertreten)/i.test(line)) return false;
	if (/^\d{5}/.test(line)) return false;
	if (/@/.test(line)) return false;
	if (/(?:Dr\.|Prof\.|Dipl\.)/i.test(line)) return true;
	if (/(?:Herr|Frau)\s+/i.test(line)) return true;
	const words = line.split(/\s+/);
	const capitalWords = words.filter(w => /^[A-ZÄÖÜ][a-zäöüß]+$/.test(w) || /^[A-ZÄÖÜ][a-zäöüß]+-[A-ZÄÖÜ][a-zäöüß]+$/.test(w));
	if (capitalWords.length >= 2 && words.length <= 5) return true;
	return false;
}

function extractEmail(html: string, businessText: string): string | null {
	// 1) mailto links in the impressum portion of HTML
	const htmlLower = html.toLowerCase();
	let impressumStart = htmlLower.indexOf('impressum');
	if (impressumStart === -1) impressumStart = 0;
	const impressumHtml = html.substring(impressumStart);

	const mailtoMatch = impressumHtml.match(/mailto:([^\s"'<>?]+)/i);
	if (mailtoMatch) {
		const email = mailtoMatch[1].replace(/&#64;/g, '@').replace(/&#46;/g, '.');
		if (!isChamberEmail(email)) return email;
	}

	// 2) Obfuscated emails
	const obfuscatedPatterns = [
		/[\w.-]+\s*\(a\)\s*[\w.-]+\.\w{2,}/i,
		/[\w.-]+\s*\[at\]\s*[\w.-]+\.\w{2,}/i,
		/[\w.-]+\s*\(at\)\s*[\w.-]+\.\w{2,}/i,
	];
	for (const p of obfuscatedPatterns) {
		const m = businessText.match(p);
		if (m) {
			const email = m[0].replace(/\s*\(a\)\s*/gi, '@').replace(/\s*\[at\]\s*/gi, '@').replace(/\s*\(at\)\s*/gi, '@');
			if (!isChamberEmail(email)) return email;
		}
	}

	// 3) Plain text email in business section
	const emailRegex = /[\w.-]+@[\w.-]+\.\w{2,}/g;
	let emailMatch;
	while ((emailMatch = emailRegex.exec(businessText)) !== null) {
		if (!isChamberEmail(emailMatch[0])) return emailMatch[0];
	}

	// 4) Fallback: any mailto in full HTML
	const fallback = html.match(/mailto:([^\s"'<>?]+)/i);
	if (fallback) return fallback[1].replace(/&#64;/g, '@').replace(/&#46;/g, '.');

	return null;
}

function isChamberEmail(email: string): boolean {
	const chamberDomains = [
		'zaek-sh.de', 'kzv-sh.de', 'zaek.de', 'kzv.de', 'lzk.de',
		'zaek-nr.de', 'kzvb.de', 'lzkh.de', 'bzaek.de',
	];
	return chamberDomains.some(d => email.toLowerCase().includes(d));
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

function extractAddress(businessText: string): { street: string | null; postalCode: string | null; city: string | null } {
	const lines = businessText.split('\n').map(l => l.trim()).filter(l => l.length > 0);

	// Find line with German PLZ (5-digit number followed by city name)
	for (let i = 0; i < lines.length; i++) {
		const plzMatch = lines[i].match(/^(\d{5})\s+([A-ZÄÖÜ][a-zäöüß]+(?:[\s-][A-Za-zäöüßÄÖÜ]+)*)$/);
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

	// Fallback: comma-separated format "Street Nr, PLZ City"
	const commaMatch = businessText.match(/([A-ZÄÖÜ][a-zäöüß]+(?:[-\s]\w+)*\s+\d+\s*[a-z]?)\s*,\s*(\d{5})\s+([A-ZÄÖÜ][a-zäöüß]+(?:[\s-]\w+)*)/i);
	if (commaMatch) {
		return {
			street: commaMatch[1].trim(),
			postalCode: commaMatch[2],
			city: commaMatch[3].trim(),
		};
	}

	// Fallback: inline "Street Nr PLZ City"
	const inlineMatch = businessText.match(/([A-ZÄÖÜ][a-zäöüß]+(?:[-\s][A-Za-zäöüßÄÖÜ]+)*(?:str(?:aße|\.)|straße|stra[sß]e|weg|allee|platz|ring|gasse|damm|berg)\s*\d+\s*[a-zA-Z]?)\s*[,\n]\s*(\d{5})\s+([A-ZÄÖÜ][a-zäöüß]+(?:\s+\w+)*)/i);
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
		if (m) { number = m[1].trim(); break; }
	}

	return {
		court: courtMatch ? courtMatch[1].trim() : null,
		number,
	};
}

function extractChamber(regulatory: string): string | null {
	// First try explicit label patterns
	const labelMatch = regulatory.match(/(?:Zuständige\s+(?:Ärzte)?[Kk]ammer|Kammer)\s*[.:]\s*\n?\s*([^\n]+)/i);
	if (labelMatch) return labelMatch[1].trim();

	// Then try to find chamber names directly (use [ \t]+ to avoid crossing newlines)
	const directMatch = regulatory.match(/((?:Landes)?[Zz]ahnärztekammer[ \t]+[\w-]+(?:[ \t]+[\w-]+)?)/);
	if (directMatch) return directMatch[1].trim();

	const aeMatch = regulatory.match(/(Ärztekammer[ \t]+[\w-]+(?:[ \t]+[\w-]+)?)/);
	if (aeMatch) return aeMatch[1].trim();

	return null;
}

function extractSupervisoryAuthority(regulatory: string): string | null {
	const labelMatch = regulatory.match(/(?:Aufsichtsbehörde|Zuständige\s+(?:Aufsichts)?[Bb]ehörde)\s*[.:]\s*\n?\s*([^\n]+)/i);
	if (labelMatch) return labelMatch[1].trim();

	const directMatch = regulatory.match(/(Kassenzahnärztliche[ \t]+Vereinigung[ \t]+[\w-]+(?:[ \t]+[\w-]+)?)/i);
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
