import type {
	ILoadOptionsFunctions,
	INodePropertyOptions,
} from 'n8n-workflow';

export const OPENAI_MODEL_FALLBACK: INodePropertyOptions[] = [
	{ name: 'gpt-4.1-nano', value: 'gpt-4.1-nano' },
	{ name: 'gpt-4.1-mini', value: 'gpt-4.1-mini' },
	{ name: 'gpt-4.1', value: 'gpt-4.1' },
	{ name: 'gpt-4o-mini', value: 'gpt-4o-mini' },
	{ name: 'gpt-4o', value: 'gpt-4o' },
	{ name: 'o3-mini', value: 'o3-mini' },
	{ name: 'o4-mini', value: 'o4-mini' },
];

const EXCLUDE =
	/audio|image|realtime|tts|transcribe|instruct|search|codex|computer|embedding|moderation|dall-e|sora|whisper|babbage|davinci|chatgpt/i;
const SKIP_VARIANT = /\d{4}-\d{2}-\d{2}|-\d{3,4}(-|$)|-preview|-16k|-chat-latest/;

/**
 * Fetches available OpenAI chat models. Tries the openAiApi credential first,
 * then falls back to reading the openAiApiKey node property.
 */
export async function getOpenAiModels(
	this: ILoadOptionsFunctions,
): Promise<INodePropertyOptions[]> {
	let apiKey: string | undefined;

	// Try credential first (works for ImpressumScraper and any node with openAiApi credential)
	try {
		const creds = await this.getCredentials('openAiApi');
		apiKey = creds.apiKey as string;
	} catch {
		// No credential configured — try node property fallback
	}

	// Fallback: read from node property (for TeamKpiTracker which uses a direct key field)
	if (!apiKey) {
		try {
			apiKey = this.getCurrentNodeParameter('openAiApiKey') as string;
		} catch {
			// Property doesn't exist on this node
		}
	}

	if (!apiKey) return OPENAI_MODEL_FALLBACK;

	try {
		const response = await this.helpers.httpRequest({
			method: 'GET',
			url: 'https://api.openai.com/v1/models',
			headers: { Authorization: `Bearer ${apiKey}` },
			timeout: 10000,
		});

		const models: INodePropertyOptions[] = (response?.data || [])
			.map((m: { id: string }) => m.id)
			.filter((id: string) => {
				if (EXCLUDE.test(id)) return false;
				if (SKIP_VARIANT.test(id)) return false;
				return /^(gpt-|o[134])/.test(id);
			})
			.sort((a: string, b: string) => a.localeCompare(b))
			.map((id: string) => ({ name: id, value: id }));

		return models.length > 0 ? models : OPENAI_MODEL_FALLBACK;
	} catch {
		return OPENAI_MODEL_FALLBACK;
	}
}
