import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IDataObject,
	IHttpRequestOptions,
} from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';

export class Ringover implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Ringover',
		name: 'ringover',
		icon: 'fa:phone',
		group: ['transform'],
		version: 1,
		subtitle: '={{$parameter["operation"] + ": " + $parameter["resource"]}}',
		description: 'Interact with the Ringover telephony API (supports multiple API keys)',
		defaults: {
			name: 'Ringover',
		},
		inputs: ['main'] as const,
		outputs: ['main'] as const,
		credentials: [
			{
				name: 'ringoverApi',
				required: true,
			},
		],
		properties: [
			// ----------------------------------
			//         Resource
			// ----------------------------------
			{
				displayName: 'Resource',
				name: 'resource',
				type: 'options',
				noDataExpression: true,
				options: [
					{ name: 'Call', value: 'call' },
					{ name: 'Contact', value: 'contact' },
					{ name: 'Conversation', value: 'conversation' },
					{ name: 'SMS', value: 'sms' },
					{ name: 'Tag', value: 'tag' },
					{ name: 'Team', value: 'team' },
					{ name: 'Transcription', value: 'transcription' },
					{ name: 'User', value: 'user' },
				],
				default: 'call',
			},

			// ----------------------------------
			//         Operations
			// ----------------------------------
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['call'] } },
				options: [
					{
						name: 'Get',
						value: 'get',
						description: 'Get a call by ID',
						action: 'Get a call',
					},
					{
						name: 'Get Many',
						value: 'getMany',
						description: 'Get calls (queries all API keys)',
						action: 'Get many calls',
					},
				],
				default: 'getMany',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['contact'] } },
				options: [
					{
						name: 'Create',
						value: 'create',
						description: 'Create a contact',
						action: 'Create a contact',
					},
					{
						name: 'Delete',
						value: 'delete',
						description: 'Delete a contact',
						action: 'Delete a contact',
					},
					{
						name: 'Get',
						value: 'get',
						description: 'Get a contact by ID',
						action: 'Get a contact',
					},
					{
						name: 'Get Many',
						value: 'getMany',
						description: 'Get contacts (queries all API keys)',
						action: 'Get many contacts',
					},
					{
						name: 'Update',
						value: 'update',
						description: 'Update a contact',
						action: 'Update a contact',
					},
				],
				default: 'getMany',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['conversation'] } },
				options: [
					{
						name: 'Get',
						value: 'get',
						description: 'Get a conversation by ID',
						action: 'Get a conversation',
					},
					{
						name: 'Get Many',
						value: 'getMany',
						description: 'Get conversations (queries all API keys)',
						action: 'Get many conversations',
					},
				],
				default: 'getMany',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['sms'] } },
				options: [
					{
						name: 'Send',
						value: 'send',
						description: 'Send an SMS message',
						action: 'Send an SMS',
					},
				],
				default: 'send',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['tag'] } },
				options: [
					{
						name: 'Create',
						value: 'create',
						description: 'Create a tag',
						action: 'Create a tag',
					},
					{
						name: 'Get Many',
						value: 'getMany',
						description: 'Get all tags (queries all API keys)',
						action: 'Get many tags',
					},
				],
				default: 'getMany',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['team'] } },
				options: [
					{
						name: 'Get',
						value: 'get',
						description: 'Get full team info (queries all API keys)',
						action: 'Get team info',
					},
				],
				default: 'get',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['transcription'] } },
				options: [
					{
						name: 'Get',
						value: 'get',
						description: 'Get transcription by call ID',
						action: 'Get a transcription',
					},
					{
						name: 'Get Many',
						value: 'getMany',
						description: 'Get transcriptions (queries all API keys)',
						action: 'Get many transcriptions',
					},
				],
				default: 'getMany',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['user'] } },
				options: [
					{
						name: 'Get',
						value: 'get',
						description: 'Get a user by ID',
						action: 'Get a user',
					},
					{
						name: 'Get Many',
						value: 'getMany',
						description: 'Get all users (queries all API keys)',
						action: 'Get many users',
					},
				],
				default: 'getMany',
			},

			// ----------------------------------
			//         Call Parameters
			// ----------------------------------
			{
				displayName: 'Call ID',
				name: 'callId',
				type: 'string',
				required: true,
				default: '',
				displayOptions: { show: { resource: ['call'], operation: ['get'] } },
			},
			{
				displayName: 'Filters',
				name: 'filters',
				type: 'collection',
				placeholder: 'Add Filter',
				default: {},
				displayOptions: { show: { resource: ['call'], operation: ['getMany'] } },
				options: [
					{
						displayName: 'Start Date',
						name: 'start_date',
						type: 'dateTime',
						default: '',
						description: 'Filter calls from this date',
					},
					{
						displayName: 'End Date',
						name: 'end_date',
						type: 'dateTime',
						default: '',
						description: 'Filter calls until this date',
					},
					{
						displayName: 'Limit',
						name: 'limit_count',
						type: 'number',
						default: 50,
						description: 'Max number of results per key',
					},
					{
						displayName: 'Offset',
						name: 'limit_offset',
						type: 'number',
						default: 0,
					},
				],
			},

			// ----------------------------------
			//         Contact Parameters
			// ----------------------------------
			{
				displayName: 'Contact ID',
				name: 'contactId',
				type: 'string',
				required: true,
				default: '',
				displayOptions: {
					show: { resource: ['contact'], operation: ['get', 'update', 'delete'] },
				},
			},
			{
				displayName: 'First Name',
				name: 'firstName',
				type: 'string',
				default: '',
				displayOptions: { show: { resource: ['contact'], operation: ['create', 'update'] } },
			},
			{
				displayName: 'Last Name',
				name: 'lastName',
				type: 'string',
				default: '',
				displayOptions: { show: { resource: ['contact'], operation: ['create', 'update'] } },
			},
			{
				displayName: 'Company',
				name: 'company',
				type: 'string',
				default: '',
				displayOptions: { show: { resource: ['contact'], operation: ['create', 'update'] } },
			},
			{
				displayName: 'Phone Numbers',
				name: 'phoneNumbers',
				type: 'string',
				default: '',
				placeholder: '+1234567890, +0987654321',
				description: 'Comma-separated list of phone numbers in E.164 format',
				displayOptions: { show: { resource: ['contact'], operation: ['create'] } },
			},
			{
				displayName: 'Filters',
				name: 'filters',
				type: 'collection',
				placeholder: 'Add Filter',
				default: {},
				displayOptions: { show: { resource: ['contact'], operation: ['getMany'] } },
				options: [
					{ displayName: 'Search', name: 'search', type: 'string', default: '' },
					{
						displayName: 'Limit',
						name: 'limit_count',
						type: 'number',
						default: 50,
						description: 'Max results per key (max 500)',
					},
					{ displayName: 'Offset', name: 'limit_offset', type: 'number', default: 0 },
				],
			},

			// ----------------------------------
			//         Conversation Parameters
			// ----------------------------------
			{
				displayName: 'Conversation ID',
				name: 'conversationId',
				type: 'string',
				required: true,
				default: '',
				displayOptions: { show: { resource: ['conversation'], operation: ['get'] } },
			},
			{
				displayName: 'Filters',
				name: 'filters',
				type: 'collection',
				placeholder: 'Add Filter',
				default: {},
				displayOptions: { show: { resource: ['conversation'], operation: ['getMany'] } },
				options: [
					{
						displayName: 'Type',
						name: 'type',
						type: 'options',
						options: [
							{ name: 'All', value: 'ALL' },
							{ name: 'Internal', value: 'INTERNAL' },
							{ name: 'External', value: 'EXTERNAL' },
							{ name: 'Collaborative', value: 'COLLABORATIVE' },
						],
						default: 'ALL',
					},
				],
			},

			// ----------------------------------
			//         SMS Parameters
			// ----------------------------------
			{
				displayName: 'From Number',
				name: 'fromNumber',
				type: 'string',
				required: true,
				default: '',
				placeholder: '+1234567890',
				description: 'Your Ringover number in E.164 format',
				displayOptions: { show: { resource: ['sms'], operation: ['send'] } },
			},
			{
				displayName: 'To Number',
				name: 'toNumber',
				type: 'string',
				required: true,
				default: '',
				placeholder: '+0987654321',
				description: 'Recipient number in E.164 format',
				displayOptions: { show: { resource: ['sms'], operation: ['send'] } },
			},
			{
				displayName: 'Message',
				name: 'smsMessage',
				type: 'string',
				typeOptions: { rows: 3 },
				required: true,
				default: '',
				displayOptions: { show: { resource: ['sms'], operation: ['send'] } },
			},

			// ----------------------------------
			//         Tag Parameters
			// ----------------------------------
			{
				displayName: 'Tag Name',
				name: 'tagName',
				type: 'string',
				required: true,
				default: '',
				displayOptions: { show: { resource: ['tag'], operation: ['create'] } },
			},
			{
				displayName: 'Tag Color',
				name: 'tagColor',
				type: 'string',
				default: '#000000',
				description: 'Hex color for the tag',
				displayOptions: { show: { resource: ['tag'], operation: ['create'] } },
			},
			{
				displayName: 'Tag Description',
				name: 'tagDescription',
				type: 'string',
				default: '',
				displayOptions: { show: { resource: ['tag'], operation: ['create'] } },
			},

			// ----------------------------------
			//         Transcription Parameters
			// ----------------------------------
			{
				displayName: 'Call ID',
				name: 'callId',
				type: 'string',
				required: true,
				default: '',
				displayOptions: { show: { resource: ['transcription'], operation: ['get'] } },
			},
			{
				displayName: 'Filters',
				name: 'filters',
				type: 'collection',
				placeholder: 'Add Filter',
				default: {},
				displayOptions: { show: { resource: ['transcription'], operation: ['getMany'] } },
				options: [
					{ displayName: 'Start Date', name: 'start_date', type: 'dateTime', default: '' },
					{ displayName: 'End Date', name: 'end_date', type: 'dateTime', default: '' },
				],
			},

			// ----------------------------------
			//         User Parameters
			// ----------------------------------
			{
				displayName: 'User ID',
				name: 'userId',
				type: 'string',
				required: true,
				default: '',
				displayOptions: { show: { resource: ['user'], operation: ['get'] } },
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const resource = this.getNodeParameter('resource', 0) as string;
		const operation = this.getNodeParameter('operation', 0) as string;

		const credentials = await this.getCredentials('ringoverApi');
		const region = (credentials.region as string) || 'eu';
		const baseUrl =
			region === 'us'
				? 'https://public-api-us.ringover.com/v2'
				: 'https://public-api.ringover.com/v2';

		const allKeys = getAllApiKeys(credentials);

		for (let i = 0; i < items.length; i++) {
			try {
				let results: IDataObject[] = [];

				// ==================== CALL ====================
				if (resource === 'call') {
					if (operation === 'getMany') {
						const filters = this.getNodeParameter('filters', i) as IDataObject;
						const qs = buildQs(filters);
						const merged = await requestAllKeys(this, allKeys, baseUrl, 'GET', '/calls', qs);
						for (const resp of merged) {
							const callList = (resp.data as IDataObject)?.call_list;
							if (Array.isArray(callList)) {
								for (const call of callList as IDataObject[]) {
									results.push({ ...call, _keyIndex: resp._keyIndex });
								}
							} else {
								results.push(resp.data as IDataObject);
							}
						}
					} else if (operation === 'get') {
						const callId = this.getNodeParameter('callId', i) as string;
						const resp = await requestFirstSuccess(
							this,
							allKeys,
							baseUrl,
							'GET',
							`/calls/${callId}`,
						);
						results.push(resp);
					}
				}

				// ==================== CONTACT ====================
				else if (resource === 'contact') {
					if (operation === 'getMany') {
						const filters = this.getNodeParameter('filters', i) as IDataObject;
						const qs = buildQs(filters);
						const merged = await requestAllKeys(
							this,
							allKeys,
							baseUrl,
							'GET',
							'/contacts',
							qs,
						);
						for (const resp of merged) {
							const contactList = (resp.data as IDataObject)?.contact_list;
							if (Array.isArray(contactList)) {
								for (const contact of contactList as IDataObject[]) {
									results.push({ ...contact, _keyIndex: resp._keyIndex });
								}
							} else {
								results.push(resp.data as IDataObject);
							}
						}
					} else if (operation === 'get') {
						const contactId = this.getNodeParameter('contactId', i) as string;
						const resp = await requestFirstSuccess(
							this,
							allKeys,
							baseUrl,
							'GET',
							`/contacts/${contactId}`,
						);
						results.push(resp);
					} else if (operation === 'create') {
						const firstName = this.getNodeParameter('firstName', i, '') as string;
						const lastName = this.getNodeParameter('lastName', i, '') as string;
						const company = this.getNodeParameter('company', i, '') as string;
						const phoneNumbers = this.getNodeParameter('phoneNumbers', i, '') as string;

						const body: IDataObject = {};
						if (firstName) body.firstname = firstName;
						if (lastName) body.lastname = lastName;
						if (company) body.company = company;
						if (phoneNumbers) {
							body.numbers = phoneNumbers
								.split(',')
								.map((n) => n.trim())
								.filter((n) => n)
								.map((n) => ({ number: n, type: 'MOBILE' }));
						}

						const resp = (await ringoverRequest(
							this,
							allKeys[0],
							baseUrl,
							'POST',
							'/contacts',
							body,
						)) as IDataObject;
						results.push(resp);
					} else if (operation === 'update') {
						const contactId = this.getNodeParameter('contactId', i) as string;
						const firstName = this.getNodeParameter('firstName', i, '') as string;
						const lastName = this.getNodeParameter('lastName', i, '') as string;
						const company = this.getNodeParameter('company', i, '') as string;

						const body: IDataObject = {};
						if (firstName) body.firstname = firstName;
						if (lastName) body.lastname = lastName;
						if (company) body.company = company;

						const resp = (await ringoverRequest(
							this,
							allKeys[0],
							baseUrl,
							'PUT',
							`/contacts/${contactId}`,
							body,
						)) as IDataObject;
						results.push(resp);
					} else if (operation === 'delete') {
						const contactId = this.getNodeParameter('contactId', i) as string;
						const resp = (await ringoverRequest(
							this,
							allKeys[0],
							baseUrl,
							'DELETE',
							`/contacts/${contactId}`,
						)) as IDataObject;
						results.push(resp);
					}
				}

				// ==================== CONVERSATION ====================
				else if (resource === 'conversation') {
					if (operation === 'getMany') {
						const filters = this.getNodeParameter('filters', i) as IDataObject;
						const qs = buildQs(filters);
						const merged = await requestAllKeys(
							this,
							allKeys,
							baseUrl,
							'GET',
							'/conversations',
							qs,
						);
						for (const resp of merged) {
							const convList = (resp.data as IDataObject)?.conversation_list;
							if (Array.isArray(convList)) {
								for (const conv of convList as IDataObject[]) {
									results.push({ ...conv, _keyIndex: resp._keyIndex });
								}
							} else {
								results.push(resp.data as IDataObject);
							}
						}
					} else if (operation === 'get') {
						const convId = this.getNodeParameter('conversationId', i) as string;
						const resp = await requestFirstSuccess(
							this,
							allKeys,
							baseUrl,
							'GET',
							`/conversations/${convId}`,
						);
						results.push(resp);
					}
				}

				// ==================== SMS ====================
				else if (resource === 'sms') {
					if (operation === 'send') {
						const fromNumber = this.getNodeParameter('fromNumber', i) as string;
						const toNumber = this.getNodeParameter('toNumber', i) as string;
						const smsMessage = this.getNodeParameter('smsMessage', i) as string;
						const resp = (await ringoverRequest(
							this,
							allKeys[0],
							baseUrl,
							'POST',
							'/push/sms',
							{
								from_number: fromNumber,
								to_number: toNumber,
								content: smsMessage,
							},
						)) as IDataObject;
						results.push(resp);
					}
				}

				// ==================== TAG ====================
				else if (resource === 'tag') {
					if (operation === 'getMany') {
						const merged = await requestAllKeys(this, allKeys, baseUrl, 'GET', '/tags');
						for (const resp of merged) {
							const tagList = (resp.data as IDataObject)?.tag_list;
							if (Array.isArray(tagList)) {
								for (const tag of tagList as IDataObject[]) {
									results.push({ ...tag, _keyIndex: resp._keyIndex });
								}
							} else {
								results.push(resp.data as IDataObject);
							}
						}
					} else if (operation === 'create') {
						const tagName = this.getNodeParameter('tagName', i) as string;
						const tagColor = this.getNodeParameter('tagColor', i) as string;
						const tagDescription = this.getNodeParameter('tagDescription', i, '') as string;
						const resp = (await ringoverRequest(
							this,
							allKeys[0],
							baseUrl,
							'POST',
							'/tags',
							{
								name: tagName,
								color: tagColor,
								description: tagDescription,
							},
						)) as IDataObject;
						results.push(resp);
					}
				}

				// ==================== TEAM ====================
				else if (resource === 'team') {
					if (operation === 'get') {
						const merged = await requestAllKeys(this, allKeys, baseUrl, 'GET', '/teams');
						for (const resp of merged) {
							results.push({ ...(resp.data as IDataObject), _keyIndex: resp._keyIndex });
						}
					}
				}

				// ==================== TRANSCRIPTION ====================
				else if (resource === 'transcription') {
					if (operation === 'getMany') {
						const filters = this.getNodeParameter('filters', i) as IDataObject;
						const qs = buildQs(filters);
						const merged = await requestAllKeys(
							this,
							allKeys,
							baseUrl,
							'GET',
							'/transcriptions',
							qs,
						);
						for (const resp of merged) {
							const tList = (resp.data as IDataObject)?.transcription_list;
							if (Array.isArray(tList)) {
								for (const t of tList as IDataObject[]) {
									results.push({ ...t, _keyIndex: resp._keyIndex });
								}
							} else {
								results.push(resp.data as IDataObject);
							}
						}
					} else if (operation === 'get') {
						const callId = this.getNodeParameter('callId', i) as string;
						const resp = await requestFirstSuccess(
							this,
							allKeys,
							baseUrl,
							'GET',
							`/transcriptions/${callId}`,
						);
						results.push(resp);
					}
				}

				// ==================== USER ====================
				else if (resource === 'user') {
					if (operation === 'getMany') {
						const merged = await requestAllKeys(this, allKeys, baseUrl, 'GET', '/users');
						for (const resp of merged) {
							const userList = (resp.data as IDataObject)?.user_list;
							if (Array.isArray(userList)) {
								for (const u of userList as IDataObject[]) {
									results.push({ ...u, _keyIndex: resp._keyIndex });
								}
							} else {
								results.push(resp.data as IDataObject);
							}
						}
					} else if (operation === 'get') {
						const userId = this.getNodeParameter('userId', i) as string;
						const resp = await requestFirstSuccess(
							this,
							allKeys,
							baseUrl,
							'GET',
							`/users/${userId}`,
						);
						results.push(resp);
					}
				} else {
					throw new NodeOperationError(this.getNode(), `Unknown resource: ${resource}`, {
						itemIndex: i,
					});
				}

				for (const item of results) {
					returnData.push({ json: item, pairedItem: { item: i } });
				}
			} catch (error) {
				if (this.continueOnFail()) {
					returnData.push({
						json: { error: (error as Error).message },
						pairedItem: { item: i },
					});
					continue;
				}
				throw error;
			}
		}

		return [returnData];
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function getAllApiKeys(credentials: IDataObject): string[] {
	const primary = credentials.apiKey as string;
	const additionalStr = (credentials.additionalApiKeys as string) || '';
	const additional = additionalStr
		.split('\n')
		.map((k) => k.trim())
		.filter((k) => k.length > 0);
	return [primary, ...additional];
}

function buildQs(filters: IDataObject): IDataObject | undefined {
	const qs: IDataObject = {};
	let hasValues = false;
	for (const [key, value] of Object.entries(filters)) {
		if (value !== '' && value !== undefined && value !== null) {
			qs[key] = value;
			hasValues = true;
		}
	}
	return hasValues ? qs : undefined;
}

interface MultiKeyResponse {
	_keyIndex: number;
	data: IDataObject;
}

async function ringoverRequest(
	ctx: IExecuteFunctions,
	apiKey: string,
	baseUrl: string,
	method: string,
	path: string,
	body?: IDataObject,
	qs?: IDataObject,
): Promise<IDataObject> {
	const options: IHttpRequestOptions = {
		method: method as 'GET' | 'POST' | 'PUT' | 'DELETE',
		url: `${baseUrl}${path}`,
		headers: { Authorization: apiKey },
		qs: qs as Record<string, string>,
		body,
	};
	return ctx.helpers.httpRequest(options) as Promise<IDataObject>;
}

async function requestAllKeys(
	ctx: IExecuteFunctions,
	allKeys: string[],
	baseUrl: string,
	method: string,
	path: string,
	qs?: IDataObject,
	body?: IDataObject,
): Promise<MultiKeyResponse[]> {
	const results: MultiKeyResponse[] = [];

	const promises = allKeys.map(async (key, index) => {
		try {
			const data = await ringoverRequest(ctx, key, baseUrl, method, path, body, qs);
			return { _keyIndex: index, data } as MultiKeyResponse;
		} catch {
			return null;
		}
	});

	const settled = await Promise.all(promises);
	for (const result of settled) {
		if (result !== null) {
			results.push(result);
		}
	}

	return results;
}

async function requestFirstSuccess(
	ctx: IExecuteFunctions,
	allKeys: string[],
	baseUrl: string,
	method: string,
	path: string,
	qs?: IDataObject,
	body?: IDataObject,
): Promise<IDataObject> {
	let lastError: Error | undefined;
	for (const key of allKeys) {
		try {
			return await ringoverRequest(ctx, key, baseUrl, method, path, body, qs);
		} catch (error) {
			lastError = error as Error;
		}
	}
	throw new Error(
		`All API keys failed for ${method} ${path}: ${lastError?.message ?? 'Unknown error'}`,
	);
}
