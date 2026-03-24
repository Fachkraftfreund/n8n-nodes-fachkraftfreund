import type {
	IAuthenticateGeneric,
	ICredentialTestRequest,
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class SearchApi implements ICredentialType {
	name = 'searchApi';
	displayName = 'SearchAPI';
	documentationUrl = 'https://www.searchapi.io/docs/google';

	properties: INodeProperties[] = [
		{
			displayName: 'API Key',
			name: 'apiKey',
			type: 'string',
			typeOptions: { password: true },
			default: '',
			required: true,
			description: 'Your SearchAPI key from https://www.searchapi.io/dashboard',
		},
	];

	authenticate: IAuthenticateGeneric = {
		type: 'generic',
		properties: {
			qs: {
				api_key: '={{$credentials.apiKey}}',
			},
		},
	};

	test: ICredentialTestRequest = {
		request: {
			baseURL: 'https://www.searchapi.io/api/v1',
			url: '/search',
			method: 'GET',
			qs: { q: 'test', engine: 'google' },
		},
	};
}
