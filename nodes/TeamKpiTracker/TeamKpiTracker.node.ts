import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IDataObject,
	IHttpRequestOptions,
} from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';

const WALICHAT_BASE_URL = 'https://api.wali.chat/v1';
const WALICHAT_PAGE_SIZE = 200;

const GERMAN_MONTHS = [
	'Januar', 'Februar', 'März', 'April', 'Mai', 'Juni',
	'Juli', 'August', 'September', 'Oktober', 'November', 'Dezember',
];

interface BullhornSession {
	BhRestToken: string;
	restUrl: string;
}

export class TeamKpiTracker implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Team KPI Tracker',
		name: 'teamKpiTracker',
		icon: 'file:teamkpi.svg',
		group: ['transform'],
		version: 1,
		subtitle: 'Aggregate KPIs from WaliChat & Bullhorn',
		description:
			'Aggregates team KPI data from WaliChat (WhatsApp contacts) and Bullhorn (submissions, placements) for Google Sheets reporting',
		defaults: {
			name: 'Team KPI Tracker',
		},
		inputs: ['main'] as const,
		outputs: ['main'] as const,
		credentials: [
			{
				name: 'waliChatApi',
				required: true,
			},
			{
				name: 'bullhornApi',
				required: false,
			},
		],
		properties: [
			// ----------------------------------
			//         Period
			// ----------------------------------
			{
				displayName: 'Period',
				name: 'period',
				type: 'options',
				options: [
					{ name: 'Current Month', value: 'currentMonth' },
					{ name: 'Last Month', value: 'lastMonth' },
					{ name: 'Custom', value: 'custom' },
				],
				default: 'currentMonth',
				description: 'The time period to aggregate KPIs for',
			},
			{
				displayName: 'Start Date',
				name: 'customStartDate',
				type: 'dateTime',
				default: '',
				displayOptions: { show: { period: ['custom'] } },
				description: 'Start of custom period',
			},
			{
				displayName: 'End Date',
				name: 'customEndDate',
				type: 'dateTime',
				default: '',
				displayOptions: { show: { period: ['custom'] } },
				description: 'End of custom period',
			},

			// ----------------------------------
			//         Data Sources
			// ----------------------------------
			{
				displayName: 'Enable Bullhorn',
				name: 'enableBullhorn',
				type: 'boolean',
				default: true,
				description: 'Whether to fetch data from Bullhorn (Sendouts & Placements)',
			},

			// ----------------------------------
			//         WaliChat Settings
			// ----------------------------------
			{
				displayName: 'WaliChat Settings',
				name: 'waliChatSettings',
				type: 'collection',
				placeholder: 'Configure WaliChat',
				default: {},
				options: [
					{
						displayName: 'Device IDs',
						name: 'deviceIds',
						type: 'string',
						default: '',
						placeholder: 'Leave empty to auto-detect all devices',
						description:
							'Comma-separated device IDs. Leave empty to query all devices.',
					},
					{
						displayName: 'Source Labels',
						name: 'sourceLabels',
						type: 'string',
						default: 'Indeed,Stepstone,Meta Pool,Meta Kundenspezifisch',
						description:
							'Comma-separated label names used to identify contact sources. Contacts are counted per source label.',
					},
					{
						displayName: 'Positive Reply Labels',
						name: 'positiveReplyLabels',
						type: 'string',
						default: 'Lead',
						description:
							'Comma-separated label names that indicate a positive reply (e.g. "Lead", "Interessiert")',
					},
				],
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const period = this.getNodeParameter('period', 0) as string;
		const enableBullhorn = this.getNodeParameter('enableBullhorn', 0) as boolean;
		const waliChatSettings = this.getNodeParameter('waliChatSettings', 0) as IDataObject;

		// --- Compute date range ---
		const { startDate, endDate, monthName, monthNumber, year } = computePeriod(
			period,
			period === 'custom'
				? (this.getNodeParameter('customStartDate', 0) as string)
				: undefined,
			period === 'custom'
				? (this.getNodeParameter('customEndDate', 0) as string)
				: undefined,
		);

		// --- Parse label config ---
		const sourceLabelsStr =
			(waliChatSettings.sourceLabels as string) ||
			'Indeed,Stepstone,Meta Pool,Meta Kundenspezifisch';
		const sourceLabels = sourceLabelsStr
			.split(',')
			.map((l) => l.trim())
			.filter((l) => l);

		const positiveReplyLabelsStr =
			(waliChatSettings.positiveReplyLabels as string) || 'Lead';
		const positiveReplyLabels = positiveReplyLabelsStr
			.split(',')
			.map((l) => l.trim().toLowerCase())
			.filter((l) => l);

		// ==================== WALICHAT ====================
		const waliResult = await fetchWaliChatKpis.call(
			this,
			startDate,
			endDate,
			waliChatSettings,
			sourceLabels,
			positiveReplyLabels,
		);

		// ==================== BULLHORN ====================
		let bullhornResult = { submissions: 0, jobs: 0 };
		if (enableBullhorn) {
			try {
				bullhornResult = await fetchBullhornKpis.call(this, startDate, endDate);
			} catch (error) {
				if (this.continueOnFail()) {
					bullhornResult = { submissions: 0, jobs: 0 };
				} else {
					throw new NodeOperationError(
						this.getNode(),
						`Bullhorn error: ${(error as Error).message}`,
					);
				}
			}
		}

		// ==================== COMPUTE RATES ====================
		const replyRate =
			waliResult.waMessages > 0 ? waliResult.replies / waliResult.waMessages : 0;

		// Calls/conversations are 0 for now (Ringover skipped)
		const calls = 0;
		const conversations = 0;
		const reachability = calls > 0 ? conversations / calls : 0;
		const convToSubmissionRate =
			conversations > 0 ? bullhornResult.submissions / conversations : 0;

		// ==================== BUILD OUTPUT ====================
		const output: IDataObject = {
			// Period info
			month: monthName,
			monthNumber,
			year,
			periodStart: startDate.toISOString().split('T')[0],
			periodEnd: endDate.toISOString().split('T')[0],

			// WaliChat KPIs
			waMessages: waliResult.waMessages,
			replies: waliResult.replies,
			positiveReplies: waliResult.positiveReplies,

			// Ringover KPIs (placeholder)
			calls,
			conversations,

			// Bullhorn KPIs
			submissions: bullhornResult.submissions,
			jobs: bullhornResult.jobs,

			// Computed rates
			replyRate: Math.round(replyRate * 1000) / 1000,
			reachability: Math.round(reachability * 1000) / 1000,
			convToSubmissionRate: Math.round(convToSubmissionRate * 1000) / 1000,

			// Source breakdown
			...waliResult.sourceBreakdown,
		};

		return [[{ json: output }]];
	}
}

// ---------------------------------------------------------------------------
// Period computation
// ---------------------------------------------------------------------------

function computePeriod(
	period: string,
	customStart?: string,
	customEnd?: string,
): {
	startDate: Date;
	endDate: Date;
	monthName: string;
	monthNumber: number;
	year: number;
} {
	const now = new Date();

	if (period === 'custom' && customStart && customEnd) {
		const startDate = new Date(customStart);
		const endDate = new Date(customEnd);
		return {
			startDate,
			endDate,
			monthName: GERMAN_MONTHS[startDate.getMonth()],
			monthNumber: startDate.getMonth() + 1,
			year: startDate.getFullYear(),
		};
	}

	let targetMonth: number;
	let targetYear: number;

	if (period === 'lastMonth') {
		targetMonth = now.getMonth() - 1;
		targetYear = now.getFullYear();
		if (targetMonth < 0) {
			targetMonth = 11;
			targetYear--;
		}
	} else {
		targetMonth = now.getMonth();
		targetYear = now.getFullYear();
	}

	const startDate = new Date(targetYear, targetMonth, 1, 0, 0, 0, 0);
	const endDate = new Date(targetYear, targetMonth + 1, 0, 23, 59, 59, 999);

	return {
		startDate,
		endDate,
		monthName: GERMAN_MONTHS[targetMonth],
		monthNumber: targetMonth + 1,
		year: targetYear,
	};
}

// ---------------------------------------------------------------------------
// WaliChat: Fetch first contacts and aggregate KPIs
// ---------------------------------------------------------------------------

async function fetchWaliChatKpis(
	this: IExecuteFunctions,
	startDate: Date,
	endDate: Date,
	settings: IDataObject,
	sourceLabels: string[],
	positiveReplyLabels: string[],
): Promise<{
	waMessages: number;
	replies: number;
	positiveReplies: number;
	sourceBreakdown: IDataObject;
}> {
	const sinceDate = startDate.toISOString();

	// Resolve device IDs
	const deviceIdsStr = (settings.deviceIds as string) || '';
	let deviceIds: string[];
	if (deviceIdsStr.trim()) {
		deviceIds = deviceIdsStr
			.split(',')
			.map((id) => id.trim())
			.filter((id) => id);
	} else {
		const devices = (await waliChatRequest.call(
			this,
			'GET',
			'/devices',
		)) as IDataObject[];
		deviceIds = devices.map((d) => d.id as string);
	}

	// Fetch all chats from all devices
	const allChats: IDataObject[] = [];

	for (const deviceId of deviceIds) {
		let page = 0;
		let hasMore = true;

		while (hasMore) {
			const chats = (await waliChatRequest.call(
				this,
				'GET',
				`/chat/${deviceId}/chats`,
				undefined,
				{
					after: sinceDate,
					size: WALICHAT_PAGE_SIZE,
					page,
				},
			)) as IDataObject[];

			for (const chat of chats) {
				const chatDate = new Date(chat.date as string);
				if (chatDate <= endDate) {
					allChats.push(chat);
				}
			}

			hasMore = chats.length >= WALICHAT_PAGE_SIZE;
			page++;
		}
	}

	// Aggregate
	const waMessages = allChats.length;
	let replies = 0;
	let positiveReplies = 0;
	const sourceCounts: Record<string, number> = {};

	for (const label of sourceLabels) {
		sourceCounts[label] = 0;
	}
	sourceCounts['Other'] = 0;

	for (const chat of allChats) {
		const stats = chat.stats as IDataObject | undefined;
		const inbound = (stats?.inboundMessages as number) ?? 0;
		const chatLabels = (chat.labels as string[]) ?? [];

		if (inbound > 0) {
			replies++;
		}

		const hasPositiveLabel = chatLabels.some((label) =>
			positiveReplyLabels.includes(label.toLowerCase()),
		);
		if (hasPositiveLabel) {
			positiveReplies++;
		}

		let matchedSource = false;
		for (const sourceLabel of sourceLabels) {
			if (
				chatLabels.some(
					(l) => l.toLowerCase() === sourceLabel.toLowerCase(),
				)
			) {
				sourceCounts[sourceLabel]++;
				matchedSource = true;
			}
		}
		if (!matchedSource) {
			sourceCounts['Other']++;
		}
	}

	const sourceBreakdown: IDataObject = {};
	for (const [label, count] of Object.entries(sourceCounts)) {
		const key = `source_${label.replace(/\s+/g, '_')}`;
		sourceBreakdown[key] = count;
	}

	return { waMessages, replies, positiveReplies, sourceBreakdown };
}

// ---------------------------------------------------------------------------
// WaliChat API helper
// ---------------------------------------------------------------------------

async function waliChatRequest(
	this: IExecuteFunctions,
	method: string,
	path: string,
	body?: IDataObject,
	qs?: IDataObject,
): Promise<IDataObject | IDataObject[]> {
	const options: IHttpRequestOptions = {
		method: method as 'GET' | 'POST' | 'PUT' | 'DELETE',
		url: `${WALICHAT_BASE_URL}${path}`,
		qs: qs as Record<string, string>,
		body,
	};
	return this.helpers.httpRequestWithAuthentication.call(
		this,
		'waliChatApi',
		options,
	) as Promise<IDataObject | IDataObject[]>;
}

// ---------------------------------------------------------------------------
// Bullhorn: Fetch Sendouts + Placements KPIs
// ---------------------------------------------------------------------------

async function fetchBullhornKpis(
	this: IExecuteFunctions,
	startDate: Date,
	endDate: Date,
): Promise<{ submissions: number; jobs: number }> {
	const session = await bullhornAuthenticate.call(this);

	const startStr = formatBullhornDate(startDate);
	const endStr = formatBullhornDate(endDate);

	const sendoutWhere = `dateAdded >= '${startStr}' AND dateAdded <= '${endStr}'`;
	const sendouts = await bullhornQuery.call(
		this,
		session,
		'Sendout',
		sendoutWhere,
		'id,dateAdded',
	);

	const placementWhere = `dateAdded >= '${startStr}' AND dateAdded <= '${endStr}'`;
	const placements = await bullhornQuery.call(
		this,
		session,
		'Placement',
		placementWhere,
		'id,dateAdded,status',
	);

	return {
		submissions: sendouts.length,
		jobs: placements.length,
	};
}

// ---------------------------------------------------------------------------
// Bullhorn authentication
// ---------------------------------------------------------------------------

function formatBullhornDate(date: Date): string {
	const y = date.getFullYear();
	const m = String(date.getMonth() + 1).padStart(2, '0');
	const d = String(date.getDate()).padStart(2, '0');
	return `${y}-${m}-${d} 00:00:00`;
}

async function bullhornAuthenticate(
	this: IExecuteFunctions,
): Promise<BullhornSession> {
	const credentials = await this.getCredentials('bullhornApi');
	const clientId = credentials.clientId as string;
	const clientSecret = credentials.clientSecret as string;
	const username = credentials.username as string;
	const password = credentials.password as string;
	const dataCenterSetting = (credentials.dataCenter as string) || 'auto';

	// Step 1: Determine data center
	let dataCenter: string;
	if (dataCenterSetting === 'auto') {
		const loginInfoResp = (await this.helpers.httpRequest({
			method: 'GET',
			url: 'https://rest.bullhornstaffing.com/rest-services/loginInfo',
			qs: { username } as Record<string, string>,
		})) as IDataObject;

		const authUrl = loginInfoResp.oauthUrl as string;
		if (!authUrl) {
			throw new NodeOperationError(
				this.getNode(),
				'Could not determine Bullhorn data center from loginInfo',
			);
		}
		const match = authUrl.match(/auth[-.]([^.]*)?\.?bullhornstaffing\.com/);
		dataCenter = match?.[1] || '';
	} else {
		dataCenter = dataCenterSetting;
	}

	const authHost = dataCenter
		? `auth-${dataCenter}.bullhornstaffing.com`
		: 'auth.bullhornstaffing.com';
	const restHost = dataCenter
		? `rest-${dataCenter}.bullhornstaffing.com`
		: 'rest.bullhornstaffing.com';

	// Step 2: Get authorization code
	// Bullhorn's authorize endpoint redirects with the code.
	// n8n's httpRequest follows redirects by default, so the code ends up
	// in the final URL or the response body. We build the URL with
	// username/password as query params (Bullhorn's server-side login flow).
	const authorizeUrl =
		`https://${authHost}/oauth/authorize` +
		`?client_id=${encodeURIComponent(clientId)}` +
		`&response_type=code` +
		`&action=Login` +
		`&username=${encodeURIComponent(username)}` +
		`&password=${encodeURIComponent(password)}`;

	// Use the request helper with redirect handling
	let authCode: string;
	try {
		const authResp = (await this.helpers.httpRequest({
			method: 'GET',
			url: authorizeUrl,
			skipSslCertificateValidation: true,
			ignoreHttpStatusErrors: true,
		})) as string | IDataObject;

		// The response after redirect typically contains the code in the URL
		// or as a JSON body field
		const respStr =
			typeof authResp === 'string' ? authResp : JSON.stringify(authResp);
		const codeMatch = respStr.match(/code=([^&"'\s]+)/);
		if (codeMatch) {
			authCode = codeMatch[1];
		} else {
			// Try parsing as JSON with authorizationCode field
			const respObj =
				typeof authResp === 'object' ? authResp : {};
			if (respObj.authorizationCode) {
				authCode = respObj.authorizationCode as string;
			} else {
				throw new Error(
					'Could not extract authorization code from Bullhorn response',
				);
			}
		}
	} catch (error) {
		// If the redirect was to our redirect_uri with the code,
		// we might get an error but the code is in the error message/URL
		const errMsg = (error as Error).message || '';
		const codeMatch = errMsg.match(/code=([^&"'\s]+)/);
		if (codeMatch) {
			authCode = codeMatch[1];
		} else {
			throw new NodeOperationError(
				this.getNode(),
				`Bullhorn authorization failed: ${errMsg}`,
			);
		}
	}

	// Step 3: Exchange code for access token
	const tokenResp = (await this.helpers.httpRequest({
		method: 'POST',
		url: `https://${authHost}/oauth/token`,
		qs: {
			grant_type: 'authorization_code',
			code: authCode,
			client_id: clientId,
			client_secret: clientSecret,
		} as Record<string, string>,
	})) as IDataObject;

	const accessToken = tokenResp.access_token as string;
	if (!accessToken) {
		throw new NodeOperationError(
			this.getNode(),
			'Bullhorn token exchange failed: no access_token returned',
		);
	}

	// Step 4: Login to REST API
	const loginResp = (await this.helpers.httpRequest({
		method: 'POST',
		url: `https://${restHost}/rest-services/login`,
		qs: {
			version: '*',
			access_token: accessToken,
		} as Record<string, string>,
	})) as IDataObject;

	const bhRestToken = loginResp.BhRestToken as string;
	const restUrl = loginResp.restUrl as string;

	if (!bhRestToken || !restUrl) {
		throw new NodeOperationError(
			this.getNode(),
			'Bullhorn REST login failed: missing BhRestToken or restUrl',
		);
	}

	return { BhRestToken: bhRestToken, restUrl };
}

// ---------------------------------------------------------------------------
// Bullhorn query helper
// ---------------------------------------------------------------------------

async function bullhornQuery(
	this: IExecuteFunctions,
	session: BullhornSession,
	entityType: string,
	where: string,
	fields: string,
): Promise<IDataObject[]> {
	const allResults: IDataObject[] = [];
	let start = 0;
	const batchSize = 500;

	while (true) {
		const resp = (await this.helpers.httpRequest({
			method: 'GET',
			url: `${session.restUrl}query/${entityType}`,
			qs: {
				BhRestToken: session.BhRestToken,
				where,
				fields,
				count: batchSize.toString(),
				start: start.toString(),
			} as Record<string, string>,
		})) as IDataObject;

		const data = (resp.data as IDataObject[]) ?? [];
		allResults.push(...data);

		const total = (resp.total as number) ?? 0;
		start += batchSize;
		if (start >= total || data.length === 0) break;
	}

	return allResults;
}
