import type {
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class BullhornApi implements ICredentialType {
	name = 'bullhornApi';
	displayName = 'Bullhorn API';
	documentationUrl = 'https://bullhorn.github.io/rest-api-docs/';

	properties: INodeProperties[] = [
		{
			displayName: 'Client ID',
			name: 'clientId',
			type: 'string',
			default: '',
			required: true,
			description: 'Your Bullhorn API Client ID',
		},
		{
			displayName: 'Client Secret',
			name: 'clientSecret',
			type: 'string',
			typeOptions: { password: true },
			default: '',
			required: true,
			description: 'Your Bullhorn API Client Secret',
		},
		{
			displayName: 'API Username',
			name: 'username',
			type: 'string',
			default: '',
			required: true,
			description: 'Your Bullhorn API username',
		},
		{
			displayName: 'API Password',
			name: 'password',
			type: 'string',
			typeOptions: { password: true },
			default: '',
			required: true,
			description: 'Your Bullhorn API password',
		},
		{
			displayName: 'Data Center',
			name: 'dataCenter',
			type: 'options',
			options: [
				{ name: 'Auto-Detect', value: 'auto' },
				{ name: 'EU (cls33)', value: 'cls33' },
				{ name: 'US East (cls2)', value: 'cls2' },
				{ name: 'US West (cls5)', value: 'cls5' },
				{ name: 'APAC (cls21)', value: 'cls21' },
				{ name: 'UK (cls30)', value: 'cls30' },
			],
			default: 'auto',
			description: 'Bullhorn data center. Use auto-detect if unsure.',
		},
	];

	// No generic authenticate — Bullhorn uses a multi-step OAuth flow
	// handled directly in the node's execute method.
}
