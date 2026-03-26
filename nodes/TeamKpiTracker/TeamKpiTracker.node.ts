import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IDataObject,
	IHttpRequestOptions,
} from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';
import { getOpenAiModels } from '../shared/openai-models';

const WALICHAT_BASE_URL = 'https://api.wali.chat/v1';
const WALICHAT_PAGE_SIZE = 200;
const OPENAI_CLASSIFY_BATCH = 10;
const OPENAI_FETCH_CONCURRENCY = 5;

const GERMAN_MONTHS = [
	'Januar', 'Februar', 'März', 'April', 'Mai', 'Juni',
	'Juli', 'August', 'September', 'Oktober', 'November', 'Dezember',
];

interface BullhornSession {
	BhRestToken: string;
	restUrl: string;
}

interface LabelGroupCounts {
	firstMessages: number;
	replied: number;
	positiveReplies: number;
}

interface WaliChatKpiResult {
	totalFirstMessages: number;
	totalReplied: number;
	totalPositiveReplies: number;
	byLabel: Record<string, LabelGroupCounts>;
}

interface ChatWithReply {
	chatId: string;
	deviceId: string;
	outputGroup: string;
}

interface BullhornCategoryCounts {
	submissionsWeitergeleitet: number;
	jobsWeitergeleitet3Plus: number;
	jobsWeitergeleitet5Plus: number;
}

interface BullhornKpiResult {
	submissionsWeitergeleitet: number;
	jobsWeitergeleitet3Plus: number;
	jobsWeitergeleitet5Plus: number;
	bySource: Record<string, BullhornCategoryCounts>;
}

// interface RingoverCategoryCounts {
// 	callsMade: number;
// 	callsAnswered: number;
// }

// interface RingoverKpiResult {
// 	callsMade: number;
// 	callsAnswered: number;
// 	bySource: Record<string, RingoverCategoryCounts>;
// }

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

export class TeamKpiTracker implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Team KPI Tracker',
		name: 'teamKpiTracker',
		icon: 'file:teamkpi.svg',
		group: ['transform'],
		version: 1,
		subtitle: 'Aggregate daily KPIs from WaliChat, Ringover & Bullhorn',
		description:
			'Aggregates team KPI data from WaliChat (first contacts, replies, positive replies via OpenAI), Ringover (calls, conversations) and Bullhorn (submissions, pipeline stages) for reporting',
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
				displayOptions: {
					show: { enableBullhorn: [true] },
				},
			},
			// {
			// 	name: 'ringoverApi',
			// 	required: false,
			// 	displayOptions: {
			// 		show: { enableRingover: [true] },
			// 	},
			// },
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
					{ name: 'Today (Last 24h)', value: 'today' },
					{ name: 'Current Month', value: 'currentMonth' },
					{ name: 'Last Month', value: 'lastMonth' },
					{ name: 'Custom', value: 'custom' },
				],
				default: 'today',
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
				description: 'Whether to fetch data from Bullhorn (JobSubmissions & Pipeline)',
			},
			// {
			// 	displayName: 'Enable Ringover',
			// 	name: 'enableRingover',
			// 	type: 'boolean',
			// 	default: true,
			// 	description: 'Whether to fetch call data from Ringover',
			// },

			// ----------------------------------
			//         OpenAI Settings
			// ----------------------------------
			{
				displayName: 'OpenAI API Key',
				name: 'openAiApiKey',
				type: 'string',
				typeOptions: { password: true },
				default: '',
				description:
					'Your OpenAI API key for classifying positive replies. Leave empty to skip classification.',
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
				description:
					'The OpenAI model used to classify positive replies.',
			},

			// // ----------------------------------
			// //         Ringover Settings
			// // ----------------------------------
			// {
			// 	displayName: 'Ringover API Keys',
			// 	name: 'ringoverApiKeys',
			// 	type: 'string',
			// 	typeOptions: { password: true, rows: 4 },
			// 	default: '',
			// 	displayOptions: { show: { enableRingover: [true] } },
			// 	description:
			// 		'Ringover API keys, one per line. All keys are queried and results are merged (deduplicated).',
			// },

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
						default: 'Stepstone,Indeed,Meta Pool,Meta Kundenspezifisch',
						description:
							'Comma-separated WaliChat label names used to identify contact sources.',
					},
					{
						displayName: 'Label Groups',
						name: 'labelGroups',
						type: 'string',
						default: 'Meta=Meta Pool,Meta Kundenspezifisch',
						description:
							'Group multiple source labels into one output group. Format: GroupName=Label1,Label2 (one group per line).',
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
		const period = this.getNodeParameter('period', 0) as string;
		const enableBullhorn = this.getNodeParameter('enableBullhorn', 0) as boolean;
		// const enableRingover = this.getNodeParameter('enableRingover', 0) as boolean;
		const openAiModel = this.getNodeParameter('openAiModel', 0, 'gpt-4.1-nano') as string;
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
			'Stepstone,Indeed,Meta Pool,Meta Kundenspezifisch';
		const sourceLabels = sourceLabelsStr
			.split(',')
			.map((l) => l.trim())
			.filter((l) => l);

		const labelGroupsStr =
			(waliChatSettings.labelGroups as string) ||
			'Meta=Meta Pool,Meta Kundenspezifisch';
		const { labelToGroup, outputGroups } = parseLabelGroups(
			labelGroupsStr,
			sourceLabels,
		);

		// --- Resolve OpenAI key (from node property) ---
		const openAiKey = (this.getNodeParameter('openAiApiKey', 0, '') as string) || undefined;

		// ==================== WALICHAT ====================
		const chatsWithReplies: ChatWithReply[] = [];
		const waliResult = await fetchWaliChatKpis.call(
			this,
			startDate,
			endDate,
			waliChatSettings,
			sourceLabels,
			labelToGroup,
			outputGroups,
			chatsWithReplies,
		);

		// ==================== OPENAI CLASSIFICATION ====================
		if (openAiKey && chatsWithReplies.length > 0) {
			const classifications = await classifyReplies.call(
				this,
				chatsWithReplies,
				openAiKey,
				openAiModel,
			);

			for (const chat of chatsWithReplies) {
				if (classifications.get(chat.chatId)) {
					waliResult.totalPositiveReplies++;
					if (waliResult.byLabel[chat.outputGroup]) {
						waliResult.byLabel[chat.outputGroup].positiveReplies++;
					}
				}
			}
		}

		// // ==================== RINGOVER ====================
		// let ringoverResult: RingoverKpiResult = { callsMade: 0, callsAnswered: 0, bySource: {} };
		// if (enableRingover) {
		// 	try {
		// 		ringoverResult = await fetchRingoverKpis.call(this, startDate, endDate, labelToGroup, outputGroups);
		// 	} catch (error) {
		// 		if (!this.continueOnFail()) {
		// 			throw new NodeOperationError(
		// 				this.getNode(),
		// 				`Ringover error: ${(error as Error).message}`,
		// 			);
		// 		}
		// 	}
		// }

		// ==================== BULLHORN ====================
		let bullhornResult: BullhornKpiResult = {
			submissionsWeitergeleitet: 0,
			jobsWeitergeleitet3Plus: 0,
			jobsWeitergeleitet5Plus: 0,
			bySource: {},
		};
		if (enableBullhorn) {
			try {
				bullhornResult = await fetchBullhornKpis.call(this, startDate, endDate, labelToGroup, outputGroups);
			} catch (error) {
				if (!this.continueOnFail()) {
					throw new NodeOperationError(
						this.getNode(),
						`Bullhorn error: ${(error as Error).message}`,
					);
				}
			}
		}

		// ==================== BUILD OUTPUT (nested/boxed) ====================
		const walichatObj: IDataObject = {
			total: {
				firstMessages: waliResult.totalFirstMessages,
				replied: waliResult.totalReplied,
				positiveReplies: waliResult.totalPositiveReplies,
			},
		};
		for (const group of outputGroups) {
			const data = waliResult.byLabel[group] ?? {
				firstMessages: 0,
				replied: 0,
				positiveReplies: 0,
			};
			walichatObj[group] = {
				firstMessages: data.firstMessages,
				replied: data.replied,
				positiveReplies: data.positiveReplies,
			};
		}

		// const ringoverObj: IDataObject = {
		// 	total: {
		// 		callsMade: ringoverResult.callsMade,
		// 		callsAnswered: ringoverResult.callsAnswered,
		// 	},
		// };
		// for (const group of outputGroups) {
		// 	const data = ringoverResult.bySource[group] ?? {
		// 		callsMade: 0,
		// 		callsAnswered: 0,
		// 	};
		// 	ringoverObj[group] = {
		// 		callsMade: data.callsMade,
		// 		callsAnswered: data.callsAnswered,
		// 	};
		// }

		const bullhornObj: IDataObject = {
			total: {
				submissionsWeitergeleitet: bullhornResult.submissionsWeitergeleitet,
				jobsWeitergeleitet3Plus: bullhornResult.jobsWeitergeleitet3Plus,
				jobsWeitergeleitet5Plus: bullhornResult.jobsWeitergeleitet5Plus,
			},
		};
		for (const group of outputGroups) {
			const data = bullhornResult.bySource[group] ?? {
				submissionsWeitergeleitet: 0,
				jobsWeitergeleitet3Plus: 0,
				jobsWeitergeleitet5Plus: 0,
			};
			bullhornObj[group] = {
				submissionsWeitergeleitet: data.submissionsWeitergeleitet,
				jobsWeitergeleitet3Plus: data.jobsWeitergeleitet3Plus,
				jobsWeitergeleitet5Plus: data.jobsWeitergeleitet5Plus,
			};
		}

		const output: IDataObject = {
			period: {
				date: endDate.toISOString().split('T')[0],
				month: monthName,
				monthNumber,
				year,
				periodStart: startDate.toISOString(),
				periodEnd: endDate.toISOString(),
			},
			walichat: walichatObj,
			// ringover: ringoverObj,
			bullhorn: bullhornObj,
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

	if (period === 'today') {
		const endDate = now;
		const startDate = new Date(now.getTime() - 24 * 60 * 60 * 1000);
		return {
			startDate,
			endDate,
			monthName: GERMAN_MONTHS[endDate.getMonth()],
			monthNumber: endDate.getMonth() + 1,
			year: endDate.getFullYear(),
		};
	}

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
// Label grouping
// ---------------------------------------------------------------------------

function parseLabelGroups(
	groupsStr: string,
	sourceLabels: string[],
): {
	labelToGroup: Record<string, string>;
	outputGroups: string[];
} {
	const labelToGroup: Record<string, string> = {};
	const outputGroupsSet = new Set<string>();

	// Parse explicit groups (format: GroupName=Label1,Label2\nGroupName2=...)
	for (const line of groupsStr.split('\n')) {
		const trimmed = line.trim();
		if (!trimmed || !trimmed.includes('=')) continue;
		const [groupName, labelsStr] = trimmed.split('=', 2);
		const group = groupName.trim();
		const labels = labelsStr
			.split(',')
			.map((l) => l.trim())
			.filter((l) => l);
		for (const label of labels) {
			labelToGroup[label.toLowerCase()] = group;
		}
		outputGroupsSet.add(group);
	}

	// Any source label not in a group maps to itself
	for (const label of sourceLabels) {
		if (!labelToGroup[label.toLowerCase()]) {
			labelToGroup[label.toLowerCase()] = label;
			outputGroupsSet.add(label);
		}
	}

	// Add 'Other' for chats without matching labels
	outputGroupsSet.add('Other');

	return {
		labelToGroup,
		outputGroups: Array.from(outputGroupsSet),
	};
}

// ---------------------------------------------------------------------------
// Source categorization (shared by Bullhorn & Ringover)
// ---------------------------------------------------------------------------

function categorizeSource(
	value: string,
	labelToGroup: Record<string, string>,
): string {
	const lower = value.toLowerCase().trim();
	if (!lower) return 'Other';
	// Exact match first
	if (labelToGroup[lower]) return labelToGroup[lower];
	// Substring match for flexibility (e.g., "Indeed.com Bewerbung" → Indeed)
	for (const [label, group] of Object.entries(labelToGroup)) {
		if (lower.includes(label) || label.includes(lower)) {
			return group;
		}
	}
	return 'Other';
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
	labelToGroup: Record<string, string>,
	outputGroups: string[],
	chatsWithRepliesOut: ChatWithReply[],
): Promise<WaliChatKpiResult> {
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
	const allChats: Array<{ chat: IDataObject; deviceId: string }> = [];

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
					allChats.push({ chat, deviceId });
				}
			}

			hasMore = chats.length >= WALICHAT_PAGE_SIZE;
			page++;
		}
	}

	// Filter out groups — only keep direct contacts (type === 'chat')
	const directChats = allChats.filter(({ chat }) => {
		const chatType = chat.type as string | undefined;
		return !chatType || chatType === 'chat';
	});

	// Initialize per-group counters
	const byLabel: Record<string, LabelGroupCounts> = {};
	for (const group of outputGroups) {
		byLabel[group] = { firstMessages: 0, replied: 0, positiveReplies: 0 };
	}

	let totalFirstMessages = 0;
	let totalReplied = 0;

	for (const { chat, deviceId } of directChats) {
		const chatLabels = (chat.labels as string[]) ?? [];

		// WaliChat stats.inboundMessages is often 0 even when there are replies.
		// Use lastInboundMessageAt as the reliable indicator for whether the
		// contact has replied.
		const hasReply = !!(chat.lastInboundMessageAt as string | null);

		// Determine output group for this chat
		let matchedGroup: string | undefined;
		for (const chatLabel of chatLabels) {
			const group = labelToGroup[chatLabel.toLowerCase()];
			if (group) {
				matchedGroup = group;
				break;
			}
		}
		const outputGroup = matchedGroup ?? 'Other';

		// Count first message
		totalFirstMessages++;
		if (byLabel[outputGroup]) {
			byLabel[outputGroup].firstMessages++;
		}

		// Count reply
		if (hasReply) {
			totalReplied++;
			if (byLabel[outputGroup]) {
				byLabel[outputGroup].replied++;
			}

			// Collect for OpenAI classification
			chatsWithRepliesOut.push({
				chatId: chat.id as string,
				deviceId,
				outputGroup,
			});
		}
	}

	return {
		totalFirstMessages,
		totalReplied,
		totalPositiveReplies: 0, // Filled in by OpenAI step
		byLabel,
	};
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
// OpenAI: Classify replies as positive/negative
// ---------------------------------------------------------------------------

async function classifyReplies(
	this: IExecuteFunctions,
	chatsWithReplies: ChatWithReply[],
	openAiKey: string,
	model: string,
): Promise<Map<string, boolean>> {
	const results = new Map<string, boolean>();

	// Step 1: Fetch inbound messages for all chats (with concurrency limit)
	const chatTexts: Array<{ chatId: string; outputGroup: string; text: string }> = [];

	for (let i = 0; i < chatsWithReplies.length; i += OPENAI_FETCH_CONCURRENCY) {
		const batch = chatsWithReplies.slice(i, i + OPENAI_FETCH_CONCURRENCY);
		const fetched = await Promise.all(
			batch.map(async (chat) => {
				try {
					const text = await fetchChatInboundText.call(
						this,
						chat.deviceId,
						chat.chatId,
					);
					return { chatId: chat.chatId, outputGroup: chat.outputGroup, text };
				} catch {
					return { chatId: chat.chatId, outputGroup: chat.outputGroup, text: '' };
				}
			}),
		);
		chatTexts.push(...fetched);
	}

	// Filter out chats with no message text
	const classifiable = chatTexts.filter((c) => c.text.trim().length > 0);

	// Step 2: Batch-classify with OpenAI
	for (let i = 0; i < classifiable.length; i += OPENAI_CLASSIFY_BATCH) {
		const batch = classifiable.slice(i, i + OPENAI_CLASSIFY_BATCH);

		const chatDescriptions = batch
			.map((c, idx) => `Chat ${idx + 1}:\n${c.text.substring(0, 500)}`)
			.join('\n\n---\n\n');

		try {
			const response = (await this.helpers.httpRequest({
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
								'You classify WhatsApp replies in a recruitment context. ' +
								'A "positive" reply means the person shows interest in a job opportunity, ' +
								'asks for more details, shares their CV, agrees to a call, or otherwise ' +
								'engages constructively. A "negative" reply means the person declines, ' +
								'shows no interest, asks to be removed, or gives a non-substantive/irrelevant response. ' +
								'Return a JSON object with a "results" array of booleans, one per chat, in order.',
						},
						{
							role: 'user',
							content: `Classify each reply as positive (true) or negative (false):\n\n${chatDescriptions}`,
						},
					],
				},
				timeout: 60000,
			})) as IDataObject;

			const choices = response.choices as IDataObject[] | undefined;
			const content = (choices?.[0]?.message as IDataObject)?.content as
				| string
				| undefined;
			if (content) {
				const parsed = JSON.parse(content) as { results?: boolean[] };
				const booleans = parsed.results ?? [];
				batch.forEach((chat, idx) => {
					results.set(chat.chatId, booleans[idx] ?? false);
				});
			}
		} catch {
			// If OpenAI fails for a batch, mark all as false
			for (const chat of batch) {
				results.set(chat.chatId, false);
			}
		}
	}

	return results;
}

async function fetchChatInboundText(
	this: IExecuteFunctions,
	deviceId: string,
	chatId: string,
): Promise<string> {
	const messages = (await waliChatRequest.call(
		this,
		'GET',
		`/chat/${deviceId}/messages`,
		undefined,
		{ chat: chatId, size: 20 },
	)) as IDataObject[];

	const inboundTexts = messages
		.filter((m) => m.fromMe !== true)
		.map((m) => (m.body as string) || '')
		.filter((t) => t.trim())
		.join('\n');

	return inboundTexts;
}

// // ---------------------------------------------------------------------------
// // Ringover: Fetch call KPIs
// // ---------------------------------------------------------------------------
//
//
// async function ringoverRequest(
// 	ctx: IExecuteFunctions,
// 	apiKey: string | null,
// 	baseUrl: string,
// 	method: string,
// 	path: string,
// 	body?: IDataObject,
// 	qs?: IDataObject,
// ): Promise<IDataObject> {
// 	// Build URL with query params explicitly (matching the format that works in tests)
// 	const url = new URL(`${baseUrl}${path}`);
// 	if (qs) {
// 		for (const [k, v] of Object.entries(qs)) {
// 			url.searchParams.set(k, String(v));
// 		}
// 	}
//
// 	const options: IHttpRequestOptions = {
// 		method: method as 'GET' | 'POST' | 'PUT' | 'DELETE',
// 		url: url.toString(),
// 		body,
// 	};
// 	// Use credential-based auth for the primary key (null),
// 	// manual Authorization header for additional keys
// 	if (apiKey === null) {
// 		return ctx.helpers.httpRequestWithAuthentication.call(
// 			ctx,
// 			'ringoverApi',
// 			options,
// 		) as Promise<IDataObject>;
// 	}
// 	options.headers = { Authorization: apiKey };
// 	return ctx.helpers.httpRequest(options) as Promise<IDataObject>;
// }
//
//
// // Ringover API allows a max date range of ~14 days per request.
// // For longer periods, we split into 14-day chunks.
// const RINGOVER_MAX_RANGE_MS = 14 * 24 * 60 * 60 * 1000;
//
// async function fetchAllCallsForKey(
// 	ctx: IExecuteFunctions,
// 	apiKey: string | null,
// 	baseUrl: string,
// 	startDate: Date,
// 	endDate: Date,
// ): Promise<IDataObject[]> {
// 	const allCalls: IDataObject[] = [];
//
// 	// Split into 14-day chunks
// 	let chunkStart = startDate.getTime();
// 	const endMs = endDate.getTime();
// 	let firstRequest = true;
//
// 	while (chunkStart < endMs) {
// 		const chunkEnd = Math.min(chunkStart + RINGOVER_MAX_RANGE_MS, endMs);
// 		let offset = 0;
// 		const limit = 500;
//
// 		while (true) {
// 			const qs: IDataObject = {
// 				start_date: new Date(chunkStart).toISOString(),
// 				end_date: new Date(chunkEnd).toISOString(),
// 				limit_count: limit,
// 				limit_offset: offset,
// 			};
//
// 			let resp: IDataObject;
// 			try {
// 				resp = await ringoverRequest(ctx, apiKey, baseUrl, 'GET', '/calls', undefined, qs);
// 			} catch (error) {
// 				// Let the first request error propagate so the user sees what went wrong
// 				// (e.g. auth failure, wrong URL). Only swallow errors on subsequent pages
// 				// where we already have partial data.
// 				if (firstRequest) throw error;
// 				break;
// 			}
// 			firstRequest = false;
//
// 			const callList = (resp as IDataObject)?.call_list;
// 			if (Array.isArray(callList) && callList.length > 0) {
// 				allCalls.push(...(callList as IDataObject[]));
// 				if (callList.length < limit) break;
// 				offset += callList.length;
// 			} else {
// 				break;
// 			}
// 		}
//
// 		chunkStart = chunkEnd;
// 	}
//
// 	return allCalls;
// }
//
// async function fetchRingoverKpis(
// 	this: IExecuteFunctions,
// 	startDate: Date,
// 	endDate: Date,
// 	labelToGroup: Record<string, string>,
// 	outputGroups: string[],
// ): Promise<RingoverKpiResult> {
// 	// Read region and additional keys from credential
// 	const credentials = await this.getCredentials('ringoverApi');
// 	const additionalKeysStr = (credentials.additionalApiKeys as string) || '';
// 	const region = (credentials.region as string) || 'eu';
//
// 	// Primary key uses credential-based auth (null), additional keys use manual headers
// 	const additionalKeys: string[] = additionalKeysStr
// 		.split('\n')
// 		.map((k) => k.trim())
// 		.filter((k) => k.length > 0);
//
// 	// Also include keys from the legacy node parameter if present
// 	const legacyKeysStr = this.getNodeParameter('ringoverApiKeys', 0, '') as string;
// 	const legacyKeys = legacyKeysStr.split('\n').map((k) => k.trim()).filter((k) => k.length > 0);
// 	for (const k of legacyKeys) {
// 		if (!additionalKeys.includes(k)) additionalKeys.push(k);
// 	}
//
// 	const baseUrl = region === 'us'
// 		? 'https://public-api-us.ringover.com/v2'
// 		: 'https://public-api.ringover.com/v2';
//
// 	// Fetch all calls from all keys in parallel (with pagination per key)
// 	// Deduplicate by call_id since multiple keys in the same account see
// 	// the same calls.
// 	// Primary key (null) uses httpRequestWithAuthentication; additional keys use manual auth
// 	const allKeyArgs: Array<string | null> = [null, ...additionalKeys];
// 	const keyPromises = allKeyArgs.map((key) =>
// 		fetchAllCallsForKey(this, key, baseUrl, startDate, endDate)
// 			.catch((err) => {
// 				// Collect errors per key but don't fail the whole operation
// 				const label = key === null ? 'primary (credential)' : `key ${key.substring(0, 8)}…`;
// 				keyErrors.push(`${label}: ${(err as Error).message}`);
// 				return [] as IDataObject[];
// 			}),
// 	);
// 	const keyErrors: string[] = [];
// 	const keyResults = await Promise.all(keyPromises);
//
// 	// If ALL keys failed, throw a combined error
// 	if (keyResults.every((r) => r.length === 0) && keyErrors.length > 0) {
// 		throw new Error(`All Ringover API keys failed:\n${keyErrors.join('\n')}`);
// 	}
//
// 	const seenCallIds = new Set<string>();
// 	const allCalls: IDataObject[] = [];
// 	for (const calls of keyResults) {
// 		for (const call of calls) {
// 			const callId = call.call_id as string;
// 			if (callId && !seenCallIds.has(callId)) {
// 				seenCallIds.add(callId);
// 				allCalls.push(call);
// 			}
// 		}
// 	}
//
// 	// Count outbound calls (Ringover uses "out" for direction)
// 	const outboundCalls = allCalls.filter((call) => {
// 		const direction = ((call.direction as string) || '').toLowerCase();
// 		return direction === 'out';
// 	});
//
// 	// Initialize per-source counters
// 	const bySource: Record<string, RingoverCategoryCounts> = {};
// 	for (const group of outputGroups) {
// 		bySource[group] = { callsMade: 0, callsAnswered: 0 };
// 	}
//
// 	let callsMade = 0;
// 	let callsAnswered = 0;
//
// 	for (const call of outboundCalls) {
// 		// Extract label/tag for categorization
// 		// Try tags array first, then label string field
// 		let tagName = '';
// 		const tags = call.tags as IDataObject[] | undefined;
// 		if (Array.isArray(tags) && tags.length > 0) {
// 			tagName = (tags[0].name as string) || (tags[0].tag as string) || '';
// 		}
// 		if (!tagName) {
// 			tagName = (call.label as string) || '';
// 		}
//
// 		const category = categorizeSource(tagName, labelToGroup);
//
// 		callsMade++;
// 		if (bySource[category]) {
// 			bySource[category].callsMade++;
// 		}
//
// 		// Count calls where someone picked up: is_answered === true AND
// 		// incall_duration > 30 seconds (actual conversation time)
// 		if (call.is_answered === true) {
// 			const incallDuration = (call.incall_duration as number) ?? 0;
// 			if (incallDuration > 30) {
// 				callsAnswered++;
// 				if (bySource[category]) {
// 					bySource[category].callsAnswered++;
// 				}
// 			}
// 		}
// 	}
//
// 	return { callsMade, callsAnswered, bySource };
// }
//
// // ---------------------------------------------------------------------------
// Bullhorn: Authentication (unchanged)
// ---------------------------------------------------------------------------

function formatBullhornTimestamp(date: Date): string {
	// Bullhorn BQL expects epoch milliseconds (unquoted) for timestamp comparisons
	return date.getTime().toString();
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

	// Step 1: Discover data center URLs from loginInfo
	let authBaseUrl: string;
	let restLoginBaseUrl: string;

	if (dataCenterSetting === 'auto') {
		const loginInfo = (await this.helpers.httpRequest({
			method: 'GET',
			url: `https://rest.bullhornstaffing.com/rest-services/loginInfo?username=${encodeURIComponent(username)}`,
		})) as IDataObject;

		const oauthUrl = (loginInfo.oauthUrl as string) || '';
		const restUrl = (loginInfo.restUrl as string) || '';

		if (!oauthUrl) {
			throw new NodeOperationError(
				this.getNode(),
				'Could not determine Bullhorn data center from loginInfo',
			);
		}

		authBaseUrl = oauthUrl.replace(/\/oauth(\/authorize)?.*$/, '');
		restLoginBaseUrl = restUrl.replace(/\/rest-services\/?.*$/, '');
	} else {
		authBaseUrl = `https://auth-${dataCenterSetting}.bullhornstaffing.com`;
		restLoginBaseUrl = `https://rest-${dataCenterSetting}.bullhornstaffing.com`;
	}

	// Step 2: Get authorization code (using helpers.request with followRedirect: false
	// to capture the code from the Location header without consuming it)
	const authorizeUrl =
		`${authBaseUrl}/oauth/authorize` +
		`?client_id=${encodeURIComponent(clientId)}` +
		`&response_type=code` +
		`&action=Login` +
		`&username=${encodeURIComponent(username)}` +
		`&password=${encodeURIComponent(password)}`;

	let authCode: string | undefined;

	const authResp = await this.helpers.request({
		method: 'GET',
		uri: authorizeUrl,
		followRedirect: false,
		resolveWithFullResponse: true,
		simple: false,
	});

	// Extract code from Location header using URL parsing (auto-decodes %3A etc.)
	const location = authResp.headers?.location || '';
	if (location) {
		try {
			const locUrl = new URL(location);
			authCode = locUrl.searchParams.get('code') || undefined;
		} catch {
			const locMatch = location.match(/code=([^&]+)/);
			if (locMatch) authCode = decodeURIComponent(locMatch[1]);
		}
	}

	// Fallback: check body
	if (!authCode && authResp.body) {
		const bodyStr = typeof authResp.body === 'string'
			? authResp.body
			: JSON.stringify(authResp.body);
		const bodyMatch = bodyStr.match(/code=([^&"'\s]+)/);
		if (bodyMatch) authCode = decodeURIComponent(bodyMatch[1]);
	}

	if (!authCode) {
		throw new NodeOperationError(
			this.getNode(),
			'Bullhorn authorization failed: Could not extract authorization code',
		);
	}

	// Step 3: Exchange code for access token
	const tokenUrl =
		`${authBaseUrl}/oauth/token` +
		`?grant_type=authorization_code` +
		`&code=${encodeURIComponent(authCode)}` +
		`&client_id=${encodeURIComponent(clientId)}` +
		`&client_secret=${encodeURIComponent(clientSecret)}`;

	let tokenResp: IDataObject;
	try {
		tokenResp = (await this.helpers.httpRequest({
			method: 'POST',
			url: tokenUrl,
		})) as IDataObject;
	} catch (error) {
		throw new NodeOperationError(
			this.getNode(),
			`Bullhorn token exchange failed: ${(error as Error).message}`,
		);
	}

	const accessToken = tokenResp.access_token as string;
	if (!accessToken) {
		throw new NodeOperationError(
			this.getNode(),
			`Bullhorn token exchange failed: ${JSON.stringify(tokenResp)}`,
		);
	}

	// Step 4: Login to REST API
	const loginUrl =
		`${restLoginBaseUrl}/rest-services/login` +
		`?version=2.0` +
		`&access_token=${encodeURIComponent(accessToken)}`;

	const loginResp = (await this.helpers.httpRequest({
		method: 'POST',
		url: loginUrl,
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

// ---------------------------------------------------------------------------
// Bullhorn: Fetch KPIs (JobSubmissions & Pipeline)
// ---------------------------------------------------------------------------

async function fetchBullhornKpis(
	this: IExecuteFunctions,
	startDate: Date,
	endDate: Date,
	labelToGroup: Record<string, string>,
	outputGroups: string[],
): Promise<BullhornKpiResult> {
	const session = await bullhornAuthenticate.call(this);

	const startStr = formatBullhornTimestamp(startDate);
	const endStr = formatBullhornTimestamp(endDate);

	// Step 1: Get JobSubmissions with status "An Kunde weitergeleitet" in period
	const submissionWhere =
		`status = 'An Kunde weitergeleitet' AND dateAdded >= ${startStr} AND dateAdded <= ${endStr}`;
	const submissions = await bullhornQuery.call(
		this,
		session,
		'JobSubmission',
		submissionWhere,
		'id,dateAdded,status,jobOrder,source',
	);

	const submissionsWeitergeleitet = submissions.length;

	// Initialize per-source counters
	const bySource: Record<string, BullhornCategoryCounts> = {};
	for (const group of outputGroups) {
		bySource[group] = {
			submissionsWeitergeleitet: 0,
			jobsWeitergeleitet3Plus: 0,
			jobsWeitergeleitet5Plus: 0,
		};
	}

	// Step 2: Extract unique JobOrder IDs (total and per category)
	const jobOrderIds = new Set<number>();
	const jobOrderIdsByCategory: Record<string, Set<number>> = {};
	for (const group of outputGroups) {
		jobOrderIdsByCategory[group] = new Set();
	}

	for (const sub of submissions) {
		const sourceValue = (sub.source as string) || '';
		const category = categorizeSource(sourceValue, labelToGroup);

		if (bySource[category]) {
			bySource[category].submissionsWeitergeleitet++;
		}

		const jobOrder = sub.jobOrder as IDataObject | undefined;
		if (jobOrder?.id) {
			jobOrderIds.add(jobOrder.id as number);
			if (jobOrderIdsByCategory[category]) {
				jobOrderIdsByCategory[category].add(jobOrder.id as number);
			}
		}
	}

	let jobsWeitergeleitet3Plus = 0;
	let jobsWeitergeleitet5Plus = 0;

	if (jobOrderIds.size > 0) {
		// Step 3: Query those JobOrders for customInt1
		const idList = Array.from(jobOrderIds).join(',');
		const jobOrderWhere = `id IN (${idList})`;
		const jobOrders = await bullhornQuery.call(
			this,
			session,
			'JobOrder',
			jobOrderWhere,
			'id,customInt1',
		);

		// Build map of jobOrder id -> customInt1
		const jobOrderCustomInt = new Map<number, number>();
		for (const jo of jobOrders) {
			jobOrderCustomInt.set(jo.id as number, (jo.customInt1 as number) ?? 0);
		}

		// Step 4: Count by customInt1 threshold (total and per category)
		for (const jo of jobOrders) {
			const customInt1 = (jo.customInt1 as number) ?? 0;
			if (customInt1 >= 3) jobsWeitergeleitet3Plus++;
			if (customInt1 >= 5) jobsWeitergeleitet5Plus++;
		}

		for (const group of outputGroups) {
			for (const joId of jobOrderIdsByCategory[group]) {
				const customInt1 = jobOrderCustomInt.get(joId) ?? 0;
				if (customInt1 >= 3) bySource[group].jobsWeitergeleitet3Plus++;
				if (customInt1 >= 5) bySource[group].jobsWeitergeleitet5Plus++;
			}
		}
	}

	return {
		submissionsWeitergeleitet,
		jobsWeitergeleitet3Plus,
		jobsWeitergeleitet5Plus,
		bySource,
	};
}
