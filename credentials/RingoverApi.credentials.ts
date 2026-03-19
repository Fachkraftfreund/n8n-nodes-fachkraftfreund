import type {
	IAuthenticateGeneric,
	ICredentialTestRequest,
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class RingoverApi implements ICredentialType {
	name = 'ringoverApi';
	displayName = 'Ringover API';
	documentationUrl = 'https://developer.ringover.com/';

	properties: INodeProperties[] = [
		{
			displayName: 'Region',
			name: 'region',
			type: 'options',
			options: [
				{ name: 'Europe', value: 'eu' },
				{ name: 'United States', value: 'us' },
			],
			default: 'eu',
			description: 'The Ringover data region for your account',
		},
		{
			displayName: 'API Key',
			name: 'apiKey',
			type: 'string',
			typeOptions: { password: true },
			default: '',
			required: true,
			description: 'Your primary Ringover API key',
		},
		{
			displayName: 'Additional API Keys',
			name: 'additionalApiKeys',
			type: 'string',
			typeOptions: { rows: 5 },
			default: '',
			description:
				'Additional Ringover API keys, one per line. Read operations will query all keys and merge results.',
		},
	];

	authenticate: IAuthenticateGeneric = {
		type: 'generic',
		properties: {
			headers: {
				Authorization: '={{$credentials.apiKey}}',
			},
		},
	};

	test: ICredentialTestRequest = {
		request: {
			baseURL:
				'={{$credentials.region === "us" ? "https://public-api-us.ringover.com/v2" : "https://public-api.ringover.com/v2"}}',
			url: '/teams',
			method: 'GET',
		},
	};
}
