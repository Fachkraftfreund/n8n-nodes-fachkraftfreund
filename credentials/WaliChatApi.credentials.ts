import type {
	IAuthenticateGeneric,
	ICredentialTestRequest,
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class WaliChatApi implements ICredentialType {
	name = 'waliChatApi';
	displayName = 'WaliChat API';
	documentationUrl = 'https://app.wali.chat/docs/';

	properties: INodeProperties[] = [
		{
			displayName: 'API Key',
			name: 'apiKey',
			type: 'string',
			typeOptions: { password: true },
			default: '',
			required: true,
			description: 'Your WaliChat API key from https://app.wali.chat/apikeys',
		},
	];

	authenticate: IAuthenticateGeneric = {
		type: 'generic',
		properties: {
			headers: {
				Token: '={{$credentials.apiKey}}',
			},
		},
	};

	test: ICredentialTestRequest = {
		request: {
			baseURL: 'https://api.wali.chat/v1',
			url: '/devices',
			method: 'GET',
		},
	};
}
