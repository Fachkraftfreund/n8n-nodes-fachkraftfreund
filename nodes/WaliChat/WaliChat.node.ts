import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IDataObject,
	IHttpRequestOptions,
} from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';

const BASE_URL = 'https://api.wali.chat/v1';

export class WaliChat implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'WaliChat',
		name: 'waliChat',
		icon: 'fa:comments',
		group: ['transform'],
		version: 1,
		subtitle: '={{$parameter["operation"] + ": " + $parameter["resource"]}}',
		description: 'Interact with the WaliChat WhatsApp API',
		defaults: {
			name: 'WaliChat',
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
			// ----------------------------------
			//         Resource
			// ----------------------------------
			{
				displayName: 'Resource',
				name: 'resource',
				type: 'options',
				noDataExpression: true,
				options: [
					{ name: 'Message', value: 'message' },
					{ name: 'Chat', value: 'chat' },
					{ name: 'Contact', value: 'contact' },
					{ name: 'Device', value: 'device' },
					{ name: 'Number Check', value: 'numberCheck' },
				],
				default: 'message',
			},

			// ----------------------------------
			//         Operations
			// ----------------------------------
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['message'] } },
				options: [
					{ name: 'Send', value: 'send', description: 'Send a message', action: 'Send a message' },
					{ name: 'Get', value: 'get', description: 'Get a message by ID', action: 'Get a message' },
					{
						name: 'Get Many',
						value: 'getMany',
						description: 'Get many messages',
						action: 'Get many messages',
					},
					{
						name: 'Delete',
						value: 'delete',
						description: 'Delete a message',
						action: 'Delete a message',
					},
				],
				default: 'send',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['chat'] } },
				options: [
					{ name: 'Get', value: 'get', description: 'Get a chat by ID', action: 'Get a chat' },
					{
						name: 'Get Many',
						value: 'getMany',
						description: 'Get many chats',
						action: 'Get many chats',
					},
					{
						name: 'Get Messages',
						value: 'getMessages',
						description: 'Get messages from a chat',
						action: 'Get messages from a chat',
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
						name: 'Get',
						value: 'get',
						description: 'Get a contact by ID',
						action: 'Get a contact',
					},
					{
						name: 'Get Many',
						value: 'getMany',
						description: 'Get many contacts',
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
				displayOptions: { show: { resource: ['device'] } },
				options: [
					{
						name: 'Get',
						value: 'get',
						description: 'Get a device by ID',
						action: 'Get a device',
					},
					{
						name: 'Get Many',
						value: 'getMany',
						description: 'Get all devices',
						action: 'Get many devices',
					},
				],
				default: 'getMany',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['numberCheck'] } },
				options: [
					{
						name: 'Check',
						value: 'check',
						description: 'Check if a phone number exists on WhatsApp',
						action: 'Check a phone number',
					},
				],
				default: 'check',
			},

			// ----------------------------------
			//         Message: Send
			// ----------------------------------
			{
				displayName: 'Device ID',
				name: 'deviceId',
				type: 'string',
				required: true,
				default: '',
				description: 'The device ID to send from (24-character hex string)',
				displayOptions: { show: { resource: ['message'], operation: ['send'] } },
			},
			{
				displayName: 'Phone Number',
				name: 'phone',
				type: 'string',
				required: true,
				default: '',
				placeholder: '+1234567890',
				description: 'Recipient phone number in E.164 format',
				displayOptions: { show: { resource: ['message'], operation: ['send'] } },
			},
			{
				displayName: 'Message Type',
				name: 'messageType',
				type: 'options',
				options: [
					{ name: 'Text', value: 'text' },
					{ name: 'Image', value: 'image' },
					{ name: 'Document', value: 'document' },
					{ name: 'Audio', value: 'audio' },
					{ name: 'Video', value: 'video' },
					{ name: 'Location', value: 'location' },
				],
				default: 'text',
				displayOptions: { show: { resource: ['message'], operation: ['send'] } },
			},
			{
				displayName: 'Message',
				name: 'message',
				type: 'string',
				typeOptions: { rows: 4 },
				default: '',
				description: 'The text message to send',
				displayOptions: {
					show: { resource: ['message'], operation: ['send'], messageType: ['text'] },
				},
			},
			{
				displayName: 'Media URL',
				name: 'mediaUrl',
				type: 'string',
				default: '',
				description: 'URL of the media file to send',
				displayOptions: {
					show: {
						resource: ['message'],
						operation: ['send'],
						messageType: ['image', 'document', 'audio', 'video'],
					},
				},
			},
			{
				displayName: 'Caption',
				name: 'caption',
				type: 'string',
				default: '',
				description: 'Caption for the media message',
				displayOptions: {
					show: {
						resource: ['message'],
						operation: ['send'],
						messageType: ['image', 'document', 'video'],
					},
				},
			},
			{
				displayName: 'Latitude',
				name: 'latitude',
				type: 'number',
				default: 0,
				displayOptions: {
					show: { resource: ['message'], operation: ['send'], messageType: ['location'] },
				},
			},
			{
				displayName: 'Longitude',
				name: 'longitude',
				type: 'number',
				default: 0,
				displayOptions: {
					show: { resource: ['message'], operation: ['send'], messageType: ['location'] },
				},
			},
			{
				displayName: 'Location Name',
				name: 'locationName',
				type: 'string',
				default: '',
				displayOptions: {
					show: { resource: ['message'], operation: ['send'], messageType: ['location'] },
				},
			},

			// ----------------------------------
			//         Message: Get / Delete
			// ----------------------------------
			{
				displayName: 'Message ID',
				name: 'messageId',
				type: 'string',
				required: true,
				default: '',
				displayOptions: { show: { resource: ['message'], operation: ['get', 'delete'] } },
			},

			// ----------------------------------
			//         Message: Get Many
			// ----------------------------------
			{
				displayName: 'Additional Fields',
				name: 'additionalFields',
				type: 'collection',
				placeholder: 'Add Field',
				default: {},
				displayOptions: { show: { resource: ['message'], operation: ['getMany'] } },
				options: [
					{ displayName: 'Device ID', name: 'device', type: 'string', default: '' },
					{ displayName: 'Page', name: 'page', type: 'number', default: 0 },
					{ displayName: 'Search', name: 'search', type: 'string', default: '' },
					{
						displayName: 'Size',
						name: 'size',
						type: 'number',
						default: 20,
						description: 'Results per page (1-200)',
					},
					{ displayName: 'Status', name: 'status', type: 'string', default: '' },
				],
			},

			// ----------------------------------
			//         Chat: All operations need Device ID
			// ----------------------------------
			{
				displayName: 'Device ID',
				name: 'deviceId',
				type: 'string',
				required: true,
				default: '',
				description: 'The device ID (24-character hex string)',
				displayOptions: { show: { resource: ['chat'] } },
			},

			// ----------------------------------
			//         Chat: Get / Get Messages
			// ----------------------------------
			{
				displayName: 'Chat ID',
				name: 'chatId',
				type: 'string',
				required: true,
				default: '',
				description: 'The WhatsApp chat ID (e.g. 1234567890@c.us)',
				displayOptions: { show: { resource: ['chat'], operation: ['get', 'getMessages'] } },
			},

			// ----------------------------------
			//         Chat: Get Many
			// ----------------------------------
			{
				displayName: 'Additional Fields',
				name: 'additionalFields',
				type: 'collection',
				placeholder: 'Add Field',
				default: {},
				displayOptions: { show: { resource: ['chat'], operation: ['getMany'] } },
				options: [
					{ displayName: 'Page', name: 'page', type: 'number', default: 0 },
					{ displayName: 'Search', name: 'search', type: 'string', default: '' },
					{ displayName: 'Size', name: 'size', type: 'number', default: 20 },
					{
						displayName: 'Status',
						name: 'status',
						type: 'options',
						options: [
							{ name: 'All', value: '' },
							{ name: 'Active', value: 'active' },
							{ name: 'Pending', value: 'pending' },
							{ name: 'Resolved', value: 'resolved' },
							{ name: 'Archived', value: 'archived' },
						],
						default: '',
					},
					{
						displayName: 'After Date',
						name: 'after',
						type: 'dateTime',
						default: '',
						description: 'Only return chats created after this date',
					},
				],
			},

			// ----------------------------------
			//         Chat: Get Messages - Pagination
			// ----------------------------------
			{
				displayName: 'Additional Fields',
				name: 'additionalFields',
				type: 'collection',
				placeholder: 'Add Field',
				default: {},
				displayOptions: { show: { resource: ['chat'], operation: ['getMessages'] } },
				options: [
					{ displayName: 'Page', name: 'page', type: 'number', default: 0 },
					{ displayName: 'Size', name: 'size', type: 'number', default: 20 },
				],
			},

			// ----------------------------------
			//         Contact: All operations need Device ID
			// ----------------------------------
			{
				displayName: 'Device ID',
				name: 'deviceId',
				type: 'string',
				required: true,
				default: '',
				description: 'The device ID (24-character hex string)',
				displayOptions: { show: { resource: ['contact'] } },
			},

			// ----------------------------------
			//         Contact: Get / Update
			// ----------------------------------
			{
				displayName: 'Contact ID',
				name: 'contactId',
				type: 'string',
				required: true,
				default: '',
				displayOptions: { show: { resource: ['contact'], operation: ['get', 'update'] } },
			},

			// ----------------------------------
			//         Contact: Create
			// ----------------------------------
			{
				displayName: 'Phone Number',
				name: 'phone',
				type: 'string',
				required: true,
				default: '',
				placeholder: '+1234567890',
				displayOptions: { show: { resource: ['contact'], operation: ['create'] } },
			},

			// ----------------------------------
			//         Contact: Create / Update fields
			// ----------------------------------
			{
				displayName: 'Contact Fields',
				name: 'contactFields',
				type: 'collection',
				placeholder: 'Add Field',
				default: {},
				displayOptions: { show: { resource: ['contact'], operation: ['create', 'update'] } },
				options: [
					{ displayName: 'Company', name: 'company', type: 'string', default: '' },
					{ displayName: 'Description', name: 'description', type: 'string', default: '' },
					{ displayName: 'Email', name: 'email', type: 'string', default: '' },
					{ displayName: 'Name', name: 'name', type: 'string', default: '' },
				],
			},

			// ----------------------------------
			//         Contact: Get Many
			// ----------------------------------
			{
				displayName: 'Additional Fields',
				name: 'additionalFields',
				type: 'collection',
				placeholder: 'Add Field',
				default: {},
				displayOptions: { show: { resource: ['contact'], operation: ['getMany'] } },
				options: [
					{ displayName: 'Page', name: 'page', type: 'number', default: 0 },
					{ displayName: 'Search', name: 'search', type: 'string', default: '' },
					{ displayName: 'Size', name: 'size', type: 'number', default: 20 },
				],
			},

			// ----------------------------------
			//         Device: Get
			// ----------------------------------
			{
				displayName: 'Device ID',
				name: 'deviceId',
				type: 'string',
				required: true,
				default: '',
				displayOptions: { show: { resource: ['device'], operation: ['get'] } },
			},

			// ----------------------------------
			//         Number Check
			// ----------------------------------
			{
				displayName: 'Phone Number',
				name: 'phone',
				type: 'string',
				required: true,
				default: '',
				placeholder: '+1234567890',
				description: 'Phone number to check (E.164 format)',
				displayOptions: { show: { resource: ['numberCheck'] } },
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const resource = this.getNodeParameter('resource', 0) as string;
		const operation = this.getNodeParameter('operation', 0) as string;

		for (let i = 0; i < items.length; i++) {
			try {
				let responseData: IDataObject | IDataObject[];

				// ==================== MESSAGE ====================
				if (resource === 'message') {
					if (operation === 'send') {
						const deviceId = this.getNodeParameter('deviceId', i) as string;
						const phone = this.getNodeParameter('phone', i) as string;
						const messageType = this.getNodeParameter('messageType', i) as string;

						const body: IDataObject = { phone, device: deviceId };

						if (messageType === 'text') {
							body.message = this.getNodeParameter('message', i) as string;
						} else if (messageType === 'location') {
							body.location = {
								latitude: this.getNodeParameter('latitude', i),
								longitude: this.getNodeParameter('longitude', i),
								name: this.getNodeParameter('locationName', i) as string,
							};
						} else {
							body.media = {
								url: this.getNodeParameter('mediaUrl', i) as string,
							};
							const caption = this.getNodeParameter('caption', i, '') as string;
							if (caption) {
								body.message = caption;
							}
						}

						responseData = (await waliChatRequest.call(
							this,
							'POST',
							'/messages',
							body,
						)) as IDataObject;
					} else if (operation === 'get') {
						const messageId = this.getNodeParameter('messageId', i) as string;
						responseData = (await waliChatRequest.call(
							this,
							'GET',
							`/messages/${messageId}`,
						)) as IDataObject;
					} else if (operation === 'getMany') {
						const qs = buildQueryString(
							this.getNodeParameter('additionalFields', i) as IDataObject,
						);
						responseData = (await waliChatRequest.call(
							this,
							'GET',
							'/messages',
							undefined,
							qs,
						)) as IDataObject;
					} else if (operation === 'delete') {
						const messageId = this.getNodeParameter('messageId', i) as string;
						responseData = (await waliChatRequest.call(
							this,
							'DELETE',
							`/messages/${messageId}`,
						)) as IDataObject;
					} else {
						throw new NodeOperationError(
							this.getNode(),
							`Unknown operation: ${operation}`,
							{ itemIndex: i },
						);
					}
				}

				// ==================== CHAT ====================
				else if (resource === 'chat') {
					const deviceId = this.getNodeParameter('deviceId', i) as string;

					if (operation === 'get') {
						const chatId = this.getNodeParameter('chatId', i) as string;
						responseData = (await waliChatRequest.call(
							this,
							'GET',
							`/chat/${deviceId}/chats/${chatId}`,
						)) as IDataObject;
					} else if (operation === 'getMany') {
						const qs = buildQueryString(
							this.getNodeParameter('additionalFields', i) as IDataObject,
						);
						responseData = (await waliChatRequest.call(
							this,
							'GET',
							`/chat/${deviceId}/chats`,
							undefined,
							qs,
						)) as IDataObject;
					} else if (operation === 'getMessages') {
						const chatId = this.getNodeParameter('chatId', i) as string;
						const qs = buildQueryString(
							this.getNodeParameter('additionalFields', i) as IDataObject,
						);
						responseData = (await waliChatRequest.call(
							this,
							'GET',
							`/chat/${deviceId}/messages`,
							undefined,
							{ ...qs, chat: chatId },
						)) as IDataObject;
					} else {
						throw new NodeOperationError(
							this.getNode(),
							`Unknown operation: ${operation}`,
							{ itemIndex: i },
						);
					}
				}

				// ==================== CONTACT ====================
				else if (resource === 'contact') {
					const deviceId = this.getNodeParameter('deviceId', i) as string;

					if (operation === 'create') {
						const phone = this.getNodeParameter('phone', i) as string;
						const contactFields = this.getNodeParameter('contactFields', i) as IDataObject;
						const body: IDataObject = { phone, ...contactFields };
						responseData = (await waliChatRequest.call(
							this,
							'POST',
							`/chat/${deviceId}/contacts`,
							body,
						)) as IDataObject;
					} else if (operation === 'get') {
						const contactId = this.getNodeParameter('contactId', i) as string;
						responseData = (await waliChatRequest.call(
							this,
							'GET',
							`/chat/${deviceId}/contacts/${contactId}`,
						)) as IDataObject;
					} else if (operation === 'getMany') {
						const qs = buildQueryString(
							this.getNodeParameter('additionalFields', i) as IDataObject,
						);
						responseData = (await waliChatRequest.call(
							this,
							'GET',
							`/chat/${deviceId}/contacts`,
							undefined,
							qs,
						)) as IDataObject;
					} else if (operation === 'update') {
						const contactId = this.getNodeParameter('contactId', i) as string;
						const contactFields = this.getNodeParameter('contactFields', i) as IDataObject;
						responseData = (await waliChatRequest.call(
							this,
							'PATCH',
							`/chat/${deviceId}/contacts/${contactId}`,
							contactFields,
						)) as IDataObject;
					} else {
						throw new NodeOperationError(
							this.getNode(),
							`Unknown operation: ${operation}`,
							{ itemIndex: i },
						);
					}
				}

				// ==================== DEVICE ====================
				else if (resource === 'device') {
					if (operation === 'get') {
						const deviceId = this.getNodeParameter('deviceId', i) as string;
						responseData = (await waliChatRequest.call(
							this,
							'GET',
							`/devices/${deviceId}`,
						)) as IDataObject;
					} else if (operation === 'getMany') {
						responseData = (await waliChatRequest.call(
							this,
							'GET',
							'/devices',
						)) as IDataObject;
					} else {
						throw new NodeOperationError(
							this.getNode(),
							`Unknown operation: ${operation}`,
							{ itemIndex: i },
						);
					}
				}

				// ==================== NUMBER CHECK ====================
				else if (resource === 'numberCheck') {
					const phone = this.getNodeParameter('phone', i) as string;
					responseData = (await waliChatRequest.call(this, 'POST', '/numbers/exists', {
						phone,
					})) as IDataObject;
				} else {
					throw new NodeOperationError(this.getNode(), `Unknown resource: ${resource}`, {
						itemIndex: i,
					});
				}

				// Normalize output
				const items_out = Array.isArray(responseData) ? responseData : [responseData];
				for (const item of items_out) {
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

function buildQueryString(fields: IDataObject): IDataObject {
	const qs: IDataObject = {};
	for (const [key, value] of Object.entries(fields)) {
		if (value !== '' && value !== undefined && value !== null) {
			qs[key] = value;
		}
	}
	return qs;
}

async function waliChatRequest(
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
