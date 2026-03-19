import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IDataObject,
	IHttpRequestOptions,
} from 'n8n-workflow';

const BASE_URL = 'https://api.wali.chat/v1';
const PAGE_SIZE = 200;

export class WaliChatFirstContacts implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'WaliChat First Contacts',
		name: 'waliChatFirstContacts',
		icon: 'file:walichat.svg',
		group: ['transform'],
		version: 1,
		subtitle: 'Get new chats (first contacts) from the last 24h',
		description:
			'Fetches all new chats (first contacts) from the last 24 hours across all connected WaliChat devices',
		defaults: {
			name: 'WaliChat First Contacts',
		},
		inputs: ['main'] as const,
		outputs: ['main'] as const,
		credentials: [
			{
				name: 'waliChatApi',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Hours Back',
				name: 'hoursBack',
				type: 'number',
				default: 24,
				description: 'How many hours back to look for new chats (default: 24)',
			},
			{
				displayName: 'Device IDs',
				name: 'deviceIds',
				type: 'string',
				default: '',
				placeholder: 'Leave empty to auto-detect all devices',
				description:
					'Comma-separated device IDs to query. Leave empty to automatically query all connected devices.',
			},
			{
				displayName: 'Additional Filters',
				name: 'filters',
				type: 'collection',
				placeholder: 'Add Filter',
				default: {},
				options: [
					{
						displayName: 'Status',
						name: 'status',
						type: 'multiOptions',
						options: [
							{ name: 'Pending', value: 'pending' },
							{ name: 'Active', value: 'active' },
							{ name: 'Resolved', value: 'resolved' },
							{ name: 'Archived', value: 'archived' },
						],
						default: [],
						description: 'Filter by chat status',
					},
					{
						displayName: 'Chat Type',
						name: 'type',
						type: 'options',
						options: [
							{ name: 'All', value: '' },
							{ name: 'User Chat', value: 'chat' },
							{ name: 'Group', value: 'group' },
							{ name: 'Channel', value: 'channel' },
						],
						default: '',
					},
					{
						displayName: 'Only User-Initiated',
						name: 'onlyUserInitiated',
						type: 'boolean',
						default: false,
						description:
							'Whether to only return chats where the first message was sent by the user (not by the business)',
					},
					{
						displayName: 'Labels',
						name: 'labels',
						type: 'string',
						default: '',
						placeholder: 'e.g. Stepstone, Indeed',
						description: 'Filter by label keywords (comma-separated)',
					},
					{
						displayName: 'Search',
						name: 'search',
						type: 'string',
						default: '',
						description:
							'Search by phone number, name, or chat content',
					},
				],
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const hoursBack = this.getNodeParameter('hoursBack', 0) as number;
		const deviceIdsStr = this.getNodeParameter('deviceIds', 0) as string;
		const filters = this.getNodeParameter('filters', 0) as IDataObject;

		const sinceDate = new Date(Date.now() - hoursBack * 60 * 60 * 1000).toISOString();

		// Resolve device IDs
		let deviceIds: string[];
		if (deviceIdsStr.trim()) {
			deviceIds = deviceIdsStr.split(',').map((id) => id.trim()).filter((id) => id);
		} else {
			const devices = (await apiRequest.call(this, 'GET', '/devices')) as IDataObject[];
			deviceIds = devices.map((d) => d.id as string);
		}

		const returnData: INodeExecutionData[] = [];

		// Fetch chats from all devices in parallel
		const devicePromises = deviceIds.map(async (deviceId) => {
			const allChats: IDataObject[] = [];
			let page = 0;
			let hasMore = true;

			while (hasMore) {
				const qs: IDataObject = {
					after: sinceDate,
					size: PAGE_SIZE,
					page,
				};

				// Apply additional filters
				if (filters.status && (filters.status as string[]).length > 0) {
					qs.status = (filters.status as string[]).join(',');
				}
				if (filters.type) {
					qs.type = filters.type;
				}
				if (filters.labels) {
					qs.labels = filters.labels;
				}
				if (filters.search) {
					qs.search = filters.search;
				}

				const chats = (await apiRequest.call(
					this,
					'GET',
					`/chat/${deviceId}/chats`,
					undefined,
					qs,
				)) as IDataObject[];

				allChats.push(...chats);

				if (chats.length < PAGE_SIZE) {
					hasMore = false;
				} else {
					page++;
				}
			}

			return { deviceId, chats: allChats };
		});

		const deviceResults = await Promise.all(devicePromises);

		for (const { deviceId, chats } of deviceResults) {
			for (const chat of chats) {
				// Apply client-side filter: only user-initiated
				if (filters.onlyUserInitiated && !chat.isUserInitiated) {
					continue;
				}

				const contact = chat.contact as IDataObject | undefined;

				returnData.push({
					json: {
						// Flattened key fields for easy use in n8n
						chatId: chat.id,
						deviceId,
						devicePhone: (chat.device as IDataObject)?.phone ?? deviceId,
						deviceAlias: (chat.device as IDataObject)?.alias ?? '',
						chatStatus: chat.status,
						chatType: chat.type,
						createdAt: chat.date,
						firstMessageAt: chat.firstMessageAt,
						lastMessageAt: chat.lastMessageAt,
						isUserInitiated: chat.isUserInitiated,
						labels: chat.labels,
						// Contact info
						contactPhone: contact?.phone ?? '',
						contactName: contact?.info
							? (contact.info as IDataObject).fullName ??
								(contact.info as IDataObject).name ??
								''
							: contact?.name ?? '',
						contactCountry: contact?.locationInfo
							? (contact.locationInfo as IDataObject).name ?? ''
							: '',
						contactIsBusiness: contact?.meta
							? (contact.meta as IDataObject).isBusiness ?? false
							: false,
						// Stats
						inboundMessages:
							(chat.stats as IDataObject)?.inboundMessages ?? 0,
						outboundMessages:
							(chat.stats as IDataObject)?.outboundMessages ?? 0,
						unreadCount: (chat.meta as IDataObject)?.unreadCount ?? 0,
						// Agent assignment
						assignedAgent: (chat.owner as IDataObject)?.agent ?? null,
						// Full raw data for advanced use
						_raw: chat,
					},
				});
			}
		}

		return [returnData];
	}
}

async function apiRequest(
	this: IExecuteFunctions,
	method: string,
	path: string,
	body?: IDataObject,
	qs?: IDataObject,
): Promise<IDataObject | IDataObject[]> {
	const options: IHttpRequestOptions = {
		method: method as 'GET' | 'POST' | 'PUT' | 'DELETE',
		url: `${BASE_URL}${path}`,
		qs: qs as Record<string, string>,
		body,
	};
	return this.helpers.httpRequestWithAuthentication.call(
		this,
		'waliChatApi',
		options,
	) as Promise<IDataObject | IDataObject[]>;
}
